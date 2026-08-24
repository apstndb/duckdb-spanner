#!/usr/bin/env python3
"""Validate DuckDB extension metadata and MinGW PE imports without loading."""

import mmap
from pathlib import Path
import os
import struct
import sys


FOOTER_SIZE = 512
FIELD_SIZE = 32
HEADER_MARKER_OFFSET = 224
# Offsets follow the pinned extension-ci-tools append_extension_metadata.py:
# three unused fields, then ABI, extension version, DuckDB version, platform,
# and the header marker.
FIXED_FIELDS = {
    "abi": (96, "C_STRUCT_UNSTABLE"),
    "duckdb_version": (160, "v1.5.5"),
    "header_marker": (HEADER_MARKER_OFFSET, "4"),
}
PLATFORM_FIELD_OFFSET = 192
SUPPORTED_PLATFORMS = frozenset(
    {
        "linux_amd64",
        "linux_arm64",
        "osx_amd64",
        "osx_arm64",
        "windows_amd64",
        "windows_amd64_mingw",
    }
)
MINGW_PLATFORM = "windows_amd64_mingw"

# The pinned MinGW build is fully static apart from Windows system libraries.
# Keep this allow-list exact so a new runtime dependency stops release staging
# until it is reviewed, rather than surprising users on a clean Windows host.
ALLOWED_WINDOWS_IMPORTS = {
    "advapi32.dll",
    "api-ms-win-core-synch-l1-2-0.dll",
    "bcrypt.dll",
    "bcryptprimitives.dll",
    "crypt32.dll",
    "kernel32.dll",
    "ntdll.dll",
    "ws2_32.dll",
}
FORBIDDEN_MINGW_RUNTIME_IMPORTS = {
    "libgcc_s_dw2-1.dll",
    "libgcc_s_seh-1.dll",
    "libgomp-1.dll",
    "libstdc++-6.dll",
    "libwinpthread-1.dll",
}


def footer_field(footer: bytes, offset: int) -> str:
    raw = footer[offset : offset + FIELD_SIZE].split(b"\0", 1)[0]
    try:
        return raw.decode("ascii")
    except UnicodeDecodeError as error:
        raise RuntimeError(f"metadata field at offset {offset} is not ASCII") from error


def expected_metadata(platform: str, release_tag: str) -> dict[str, tuple[int, str]]:
    if platform not in SUPPORTED_PLATFORMS:
        raise RuntimeError(f"unsupported release platform: {platform}")
    if not release_tag.startswith("v") or len(release_tag) == 1:
        raise RuntimeError("EXPECTED_EXTENSION_TAG must be a v-prefixed release tag")

    return {
        **FIXED_FIELDS,
        "extension_version": (128, release_tag[1:]),
        "platform": (PLATFORM_FIELD_OFFSET, platform),
    }


def validate_footer(footer: bytes, platform: str, release_tag: str) -> dict[str, str]:
    if len(footer) != FOOTER_SIZE:
        raise RuntimeError(f"DuckDB extension footer must be {FOOTER_SIZE} bytes")

    expected = expected_metadata(platform, release_tag)
    actual = {name: footer_field(footer, offset) for name, (offset, _) in expected.items()}
    mismatches = {
        name: {"expected": value, "actual": actual[name]}
        for name, (_, value) in expected.items()
        if actual[name] != value
    }
    if mismatches:
        raise RuntimeError(f"unexpected DuckDB extension metadata: {mismatches}")
    return actual


def unpack_from(image: mmap.mmap, format_: str, offset: int, context: str) -> tuple[int, ...]:
    size = struct.calcsize(format_)
    if offset < 0 or offset + size > len(image):
        raise RuntimeError(f"truncated PE image while reading {context}")
    return struct.unpack_from(format_, image, offset)


class RvaReader:
    """Read file-backed PE bytes while preserving RVA section boundaries."""

    def __init__(
        self, image: mmap.mmap, sections: list[tuple[int, int, int, int]]
    ) -> None:
        self.image = image
        self.sections = sections

    def _section_for(self, rva: int, context: str) -> tuple[int, int, int, int]:
        matches = []
        for section in self.sections:
            virtual_address, virtual_size, raw_offset, raw_size = section
            mapped_size = max(virtual_size, raw_size)
            if virtual_address <= rva < virtual_address + mapped_size:
                matches.append(section)
        if len(matches) > 1:
            raise RuntimeError(f"{context} RVA 0x{rva:x} maps to overlapping PE sections")
        if not matches:
            raise RuntimeError(f"{context} RVA 0x{rva:x} is not mapped by a PE section")

        virtual_address, virtual_size, raw_offset, raw_size = matches[0]
        delta = rva - virtual_address
        if delta >= raw_size or raw_offset + delta >= len(self.image):
            raise RuntimeError(f"{context} points outside PE section data")
        return virtual_address, virtual_size, raw_offset, raw_size

    def read(self, rva: int, size: int, context: str) -> bytes:
        if rva < 0 or size < 0:
            raise RuntimeError(f"{context} has an invalid RVA range")
        result = bytearray()
        current_rva = rva
        remaining = size
        while remaining:
            virtual_address, virtual_size, raw_offset, raw_size = self._section_for(
                current_rva, context
            )
            delta = current_rva - virtual_address
            chunk_size = min(remaining, raw_size - delta)
            file_offset = raw_offset + delta
            if chunk_size <= 0 or file_offset + chunk_size > len(self.image):
                raise RuntimeError(f"{context} points outside PE section data")
            result.extend(self.image[file_offset : file_offset + chunk_size])
            current_rva += chunk_size
            remaining -= chunk_size
        return bytes(result)


def ascii_c_string(reader: RvaReader, rva: int, context: str) -> str:
    value = bytearray()
    for index in range(260):
        byte = reader.read(rva + index, 1, context)
        if byte == b"\0":
            try:
                return bytes(value).decode("ascii")
            except UnicodeDecodeError as error:
                raise RuntimeError(f"{context} is not ASCII") from error
        value.extend(byte)
    raise RuntimeError(f"{context} is not NUL-terminated within 260 bytes")


def import_directory(
    image: mmap.mmap,
    sections: list[tuple[int, int, int, int]],
    rva: int,
    size: int,
) -> set[str]:
    if rva == 0 or size < 20:
        raise RuntimeError("PE image has an empty import data directory")

    reader = RvaReader(image, sections)
    imports = set()
    descriptor_rva = rva
    descriptor_end = rva + size
    while descriptor_rva + 20 <= descriptor_end:
        descriptor = reader.read(descriptor_rva, 20, "import descriptor")
        if descriptor == b"\0" * 20:
            return imports
        (name_rva,) = struct.unpack_from("<I", descriptor, 12)
        imports.add(ascii_c_string(reader, name_rva, "import name").lower())
        descriptor_rva += 20
    raise RuntimeError("PE import directory has no terminating descriptor")


def delay_import_directory(
    image: mmap.mmap,
    sections: list[tuple[int, int, int, int]],
    rva: int,
    size: int,
    image_base: int,
) -> set[str]:
    if rva == 0 and size == 0:
        return set()
    if rva == 0 or size < 32:
        raise RuntimeError("PE image has an invalid delay-import data directory")

    reader = RvaReader(image, sections)
    imports = set()
    descriptor_rva = rva
    descriptor_end = rva + size
    while descriptor_rva + 32 <= descriptor_end:
        descriptor = reader.read(descriptor_rva, 32, "delay-import descriptor")
        if descriptor == b"\0" * 32:
            return imports
        attributes, name_value = struct.unpack_from("<II", descriptor)
        if attributes & ~1:
            raise RuntimeError("delay-import descriptor has unknown attributes")
        name_rva = name_value if attributes & 1 else name_value - image_base
        if name_rva < 0:
            raise RuntimeError("delay-import name VA is below the PE image base")
        imports.add(ascii_c_string(reader, name_rva, "delay-import name").lower())
        descriptor_rva += 32
    raise RuntimeError("PE delay-import directory has no terminating descriptor")


def pe_imports(image: mmap.mmap) -> set[str]:
    if len(image) < 64 or image[:2] != b"MZ":
        raise RuntimeError("artifact is not a DOS/PE image")
    (pe_offset,) = unpack_from(image, "<I", 0x3C, "PE header offset")
    if pe_offset + 24 > len(image) or image[pe_offset : pe_offset + 4] != b"PE\0\0":
        raise RuntimeError("artifact has no valid PE signature")

    machine, section_count, optional_size = unpack_from(
        image, "<HH12xH", pe_offset + 4, "COFF header"
    )
    if machine != 0x8664:
        raise RuntimeError(f"expected AMD64 COFF machine 0x8664, got 0x{machine:x}")
    optional_offset = pe_offset + 24
    (magic,) = unpack_from(image, "<H", optional_offset, "optional-header magic")
    if magic != 0x20B:
        raise RuntimeError(f"expected a PE32+ image, got optional-header magic 0x{magic:x}")
    if optional_size < 128:
        raise RuntimeError(f"PE32+ optional header is too small: {optional_size} bytes")
    (directory_count,) = unpack_from(
        image, "<I", optional_offset + 108, "data-directory count"
    )
    if directory_count < 2:
        raise RuntimeError("PE image has no import data directory")
    import_rva, import_size = unpack_from(
        image, "<II", optional_offset + 120, "import data directory"
    )
    (image_base,) = unpack_from(image, "<Q", optional_offset + 24, "PE image base")

    sections_offset = optional_offset + optional_size
    sections = []
    for index in range(section_count):
        section_offset = sections_offset + index * 40
        virtual_size, virtual_address, raw_size, raw_offset = unpack_from(
            image, "<IIII", section_offset + 8, f"section {index} header"
        )
        sections.append((virtual_address, virtual_size, raw_offset, raw_size))

    imports = import_directory(image, sections, import_rva, import_size)
    if directory_count >= 14:
        if optional_size < 224:
            raise RuntimeError("PE32+ optional header is too small for delay imports")
        delay_rva, delay_size = unpack_from(
            image, "<II", optional_offset + 216, "delay-import data directory"
        )
        imports.update(
            delay_import_directory(image, sections, delay_rva, delay_size, image_base)
        )

    if not imports:
        raise RuntimeError("PE image imports no DLLs")
    return imports


def validate_pe_imports(imports: set[str]) -> None:
    normalized = {name.lower() for name in imports}
    forbidden = sorted(normalized & FORBIDDEN_MINGW_RUNTIME_IMPORTS)
    if forbidden:
        raise RuntimeError(f"artifact dynamically imports MinGW runtimes: {forbidden}")
    unexpected = sorted(normalized - ALLOWED_WINDOWS_IMPORTS)
    if unexpected:
        raise RuntimeError(f"artifact has unreviewed PE imports: {unexpected}")


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(f"usage: {sys.argv[0]} ARTIFACT PLATFORM")

    artifact = Path(sys.argv[1]).resolve(strict=True)
    platform = sys.argv[2]
    if artifact.stat().st_size <= FOOTER_SIZE:
        raise RuntimeError(f"release artifact is too small: {artifact.stat().st_size} bytes")

    release_tag = os.environ.get("EXPECTED_EXTENSION_TAG", "")

    with artifact.open("rb") as stream, mmap.mmap(
        stream.fileno(), 0, access=mmap.ACCESS_READ
    ) as image:
        if platform == MINGW_PLATFORM:
            imports = pe_imports(image)
            validate_pe_imports(imports)
        stream.seek(-FOOTER_SIZE, 2)
        footer = stream.read(FOOTER_SIZE)

    actual = validate_footer(footer, platform, release_tag)

    if platform == MINGW_PLATFORM:
        print(f"validated {artifact.name} PE imports: {sorted(imports)}")
    print(f"validated {artifact.name} metadata: {actual}")


if __name__ == "__main__":
    main()
