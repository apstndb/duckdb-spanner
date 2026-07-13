#!/usr/bin/env python3
"""Validate MinGW PE imports and DuckDB metadata without loading the artifact."""

import mmap
from pathlib import Path
import os
import struct
import sys


FOOTER_SIZE = 512
FIELD_SIZE = 32
# Offsets follow the pinned extension-ci-tools append_extension_metadata.py:
# three unused fields, then ABI, extension version, DuckDB version, and platform.
FIELDS = {
    "abi": (96, "C_STRUCT_UNSTABLE"),
    "extension_version": (128, None),
    "duckdb_version": (160, "v1.5.4"),
    "platform": (192, "windows_amd64_mingw"),
}

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


def unpack_from(image: mmap.mmap, format_: str, offset: int, context: str) -> tuple[int, ...]:
    size = struct.calcsize(format_)
    if offset < 0 or offset + size > len(image):
        raise RuntimeError(f"truncated PE image while reading {context}")
    return struct.unpack_from(format_, image, offset)


def rva_to_file_offset(
    image: mmap.mmap, sections: list[tuple[int, int, int, int]], rva: int, context: str
) -> int:
    for virtual_address, virtual_size, raw_offset, raw_size in sections:
        mapped_size = max(virtual_size, raw_size)
        if virtual_address <= rva < virtual_address + mapped_size:
            delta = rva - virtual_address
            if delta >= raw_size or raw_offset + delta >= len(image):
                raise RuntimeError(f"{context} points outside PE section data")
            return raw_offset + delta
    raise RuntimeError(f"{context} RVA 0x{rva:x} is not mapped by a PE section")


def ascii_c_string(image: mmap.mmap, offset: int, context: str) -> str:
    if offset < 0 or offset >= len(image):
        raise RuntimeError(f"{context} starts outside the PE image")
    end = image.find(b"\0", offset, min(offset + 260, len(image)))
    if end < 0:
        raise RuntimeError(f"{context} is not NUL-terminated within 260 bytes")
    try:
        return image[offset:end].decode("ascii")
    except UnicodeDecodeError as error:
        raise RuntimeError(f"{context} is not ASCII") from error


def pe_imports(image: mmap.mmap) -> set[str]:
    if len(image) < 64 or image[:2] != b"MZ":
        raise RuntimeError("artifact is not a DOS/PE image")
    (pe_offset,) = unpack_from(image, "<I", 0x3C, "PE header offset")
    if pe_offset + 24 > len(image) or image[pe_offset : pe_offset + 4] != b"PE\0\0":
        raise RuntimeError("artifact has no valid PE signature")

    section_count, optional_size = unpack_from(
        image, "<H12xH", pe_offset + 6, "COFF header"
    )
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
    if import_rva == 0 or import_size < 20:
        raise RuntimeError("PE image has an empty import data directory")

    sections_offset = optional_offset + optional_size
    sections = []
    for index in range(section_count):
        section_offset = sections_offset + index * 40
        virtual_size, virtual_address, raw_size, raw_offset = unpack_from(
            image, "<IIII", section_offset + 8, f"section {index} header"
        )
        sections.append((virtual_address, virtual_size, raw_offset, raw_size))

    descriptor_offset = rva_to_file_offset(
        image, sections, import_rva, "import directory"
    )
    descriptor_limit = min(descriptor_offset + import_size, len(image))
    imports = set()
    while descriptor_offset + 20 <= descriptor_limit:
        descriptor = image[descriptor_offset : descriptor_offset + 20]
        if descriptor == b"\0" * 20:
            break
        (name_rva,) = unpack_from(
            image, "<I", descriptor_offset + 12, "import descriptor name"
        )
        name_offset = rva_to_file_offset(image, sections, name_rva, "import name")
        imports.add(ascii_c_string(image, name_offset, "import name").lower())
        descriptor_offset += 20
    else:
        raise RuntimeError("PE import directory has no terminating descriptor")

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
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} ARTIFACT")

    artifact = Path(sys.argv[1]).resolve(strict=True)
    if artifact.stat().st_size <= FOOTER_SIZE:
        raise RuntimeError(f"release artifact is too small: {artifact.stat().st_size} bytes")

    release_tag = os.environ.get("EXPECTED_EXTENSION_TAG", "")
    if not release_tag.startswith("v") or len(release_tag) == 1:
        raise RuntimeError("EXPECTED_EXTENSION_TAG must be a v-prefixed release tag")
    expected = dict(FIELDS)
    expected["extension_version"] = (128, release_tag[1:])

    with artifact.open("rb") as stream, mmap.mmap(
        stream.fileno(), 0, access=mmap.ACCESS_READ
    ) as image:
        imports = pe_imports(image)
        validate_pe_imports(imports)
        stream.seek(-FOOTER_SIZE, 2)
        footer = stream.read(FOOTER_SIZE)

    actual = {name: footer_field(footer, offset) for name, (offset, _) in expected.items()}
    mismatches = {
        name: {"expected": value, "actual": actual[name]}
        for name, (_, value) in expected.items()
        if actual[name] != value
    }
    if mismatches:
        raise RuntimeError(f"unexpected DuckDB extension metadata: {mismatches}")

    print(f"validated {artifact.name} PE imports: {sorted(imports)}")
    print(f"validated {artifact.name} metadata: {actual}")


if __name__ == "__main__":
    main()
