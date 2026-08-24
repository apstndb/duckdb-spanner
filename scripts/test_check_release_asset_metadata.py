import importlib.util
from pathlib import Path
import struct
import unittest


SCRIPT = Path(__file__).with_name("check-release-asset-metadata.py")
SPEC = importlib.util.spec_from_file_location("check_release_asset_metadata", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
metadata = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(metadata)


def pe_image(
    import_names: list[str],
    delay_import_names: list[str] | None = None,
    *,
    machine: int = 0x8664,
) -> bytearray:
    if delay_import_names is None:
        delay_import_names = []
    pe_offset = 0x80
    optional_size = 0xF0
    section_offset = pe_offset + 24 + optional_size
    raw_offset = 0x200
    section_rva = 0x1000
    image = bytearray(0x1000)

    image[:2] = b"MZ"
    struct.pack_into("<I", image, 0x3C, pe_offset)
    image[pe_offset : pe_offset + 4] = b"PE\0\0"
    struct.pack_into(
        "<HHIIIHH", image, pe_offset + 4, machine, 1, 0, 0, 0, optional_size, 0
    )

    optional_offset = pe_offset + 24
    struct.pack_into("<H", image, optional_offset, 0x20B)
    struct.pack_into("<Q", image, optional_offset + 24, 0x140000000)
    struct.pack_into("<I", image, optional_offset + 108, 16)
    descriptor_size = (len(import_names) + 1) * 20
    struct.pack_into(
        "<II", image, optional_offset + 120, section_rva, descriptor_size
    )
    delay_descriptor_rva = section_rva + 0x100
    delay_descriptor_size = (len(delay_import_names) + 1) * 32
    struct.pack_into(
        "<II",
        image,
        optional_offset + 216,
        delay_descriptor_rva if delay_import_names else 0,
        delay_descriptor_size if delay_import_names else 0,
    )

    image[section_offset : section_offset + 8] = b".rdata\0\0"
    struct.pack_into(
        "<IIII", image, section_offset + 8, 0x400, section_rva, 0x400, raw_offset
    )

    name_offset = raw_offset + 0x200
    for index, name in enumerate(import_names):
        struct.pack_into(
            "<I",
            image,
            raw_offset + index * 20 + 12,
            section_rva + name_offset - raw_offset,
        )
        encoded = name.encode("ascii") + b"\0"
        image[name_offset : name_offset + len(encoded)] = encoded
        name_offset += len(encoded)
    for index, name in enumerate(delay_import_names):
        descriptor_offset = raw_offset + 0x100 + index * 32
        encoded = name.encode("ascii") + b"\0"
        struct.pack_into(
            "<II",
            image,
            descriptor_offset,
            1,
            section_rva + name_offset - raw_offset,
        )
        image[name_offset : name_offset + len(encoded)] = encoded
        name_offset += len(encoded)
    return image


def contiguous_section_pe_image(*, delay: bool) -> bytearray:
    """Build descriptors across contiguous RVAs whose raw sections are split."""
    pe_offset = 0x80
    optional_size = 0xF0
    section_offset = pe_offset + 24 + optional_size
    section_rvas = [0x1000, 0x1020, 0x1040, 0x1060]
    raw_offsets = [0x300, 0x500, 0x900, 0xC00]
    image = bytearray(0xE00)

    image[:2] = b"MZ"
    struct.pack_into("<I", image, 0x3C, pe_offset)
    image[pe_offset : pe_offset + 4] = b"PE\0\0"
    struct.pack_into(
        "<HHIIIHH", image, pe_offset + 4, 0x8664, 4, 0, 0, 0, optional_size, 0
    )
    optional_offset = pe_offset + 24
    struct.pack_into("<H", image, optional_offset, 0x20B)
    struct.pack_into("<Q", image, optional_offset + 24, 0x140000000)
    struct.pack_into("<I", image, optional_offset + 108, 16)

    def rva_offset(rva: int) -> int:
        for section_rva, raw_offset in zip(section_rvas, raw_offsets):
            section_size = 0x200 if section_rva == 0x1060 else 0x20
            if section_rva <= rva < section_rva + section_size:
                return raw_offset + rva - section_rva
        raise AssertionError(f"RVA 0x{rva:x} is outside fixture sections")

    def write_rva(rva: int, data: bytes) -> None:
        for index, byte in enumerate(data):
            image[rva_offset(rva + index)] = byte

    normal_rva = 0x1100 if delay else 0x1000
    normal_size = 40 if delay else 60
    struct.pack_into("<II", image, optional_offset + 120, normal_rva, normal_size)
    delay_rva = 0x1008 if delay else 0
    delay_size = 96 if delay else 0
    struct.pack_into("<II", image, optional_offset + 216, delay_rva, delay_size)

    for index, (section_rva, raw_offset) in enumerate(zip(section_rvas, raw_offsets)):
        section_size = 0x200 if index == 3 else 0x20
        image[section_offset + index * 40 : section_offset + index * 40 + 8] = (
            f".s{index}".encode("ascii") + b"\0" * 5
        )
        struct.pack_into(
            "<IIII", image, section_offset + index * 40 + 8,
            section_size, section_rva, section_size, raw_offset
        )

    def normal_descriptor(rva: int, name_rva: int) -> None:
        descriptor = bytearray(20)
        struct.pack_into("<I", descriptor, 12, name_rva)
        write_rva(rva, descriptor)

    def delay_descriptor(rva: int, name_rva: int) -> None:
        descriptor = bytearray(32)
        struct.pack_into("<II", descriptor, 0, 1, name_rva)
        write_rva(rva, descriptor)

    if delay:
        normal_descriptor(0x1100, 0x1080)
        normal_descriptor(0x1114, 0)
        delay_descriptor(0x1008, 0x1080)
        delay_descriptor(0x1028, 0x1090)
        write_rva(0x1048, b"\0" * 32)
        write_rva(0x1080, b"KERNEL32.dll\0")
        write_rva(0x1090, b"LIBGCC_S_SEH-1.DLL\0")
    else:
        normal_descriptor(0x1000, 0x1080)
        normal_descriptor(0x1014, 0x103C)
        write_rva(0x1028, b"\0" * 20)
        write_rva(0x103C, b"LIBGCC_S_SEH-1.DLL\0")
        write_rva(0x1080, b"KERNEL32.dll\0")
    return image


def metadata_footer(platform: str, release_tag: str = "v0.4.1") -> bytearray:
    footer = bytearray(metadata.FOOTER_SIZE)
    for _, (offset, value) in metadata.expected_metadata(platform, release_tag).items():
        footer[offset : offset + len(value)] = value.encode("ascii")
    return footer


class PeImportTests(unittest.TestCase):
    def test_accepts_reviewed_windows_system_import(self) -> None:
        imports = metadata.pe_imports(pe_image(["KERNEL32.dll"]))
        self.assertEqual(imports, {"kernel32.dll"})
        metadata.validate_pe_imports(imports)

    def test_rejects_mingw_runtime_case_insensitively(self) -> None:
        imports = metadata.pe_imports(contiguous_section_pe_image(delay=False))
        self.assertEqual(imports, {"kernel32.dll", "libgcc_s_seh-1.dll"})
        with self.assertRaisesRegex(RuntimeError, "MinGW runtimes"):
            metadata.validate_pe_imports(imports)

    def test_rejects_unreviewed_import(self) -> None:
        imports = metadata.pe_imports(pe_image(["unexpected.dll"]))
        with self.assertRaisesRegex(RuntimeError, "unreviewed PE imports"):
            metadata.validate_pe_imports(imports)

    def test_rejects_non_amd64_coff_machine(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "AMD64 COFF machine"):
            metadata.pe_imports(pe_image(["KERNEL32.dll"], machine=0x14C))

    def test_collects_and_rejects_delay_loaded_mingw_runtime(self) -> None:
        imports = metadata.pe_imports(
            contiguous_section_pe_image(delay=True)
        )
        self.assertEqual(imports, {"kernel32.dll", "libgcc_s_seh-1.dll"})
        with self.assertRaisesRegex(RuntimeError, "MinGW runtimes"):
            metadata.validate_pe_imports(imports)

    def test_rejects_non_pe_input(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "not a DOS/PE image"):
            metadata.pe_imports(bytearray(64))


class FooterTests(unittest.TestCase):
    def test_accepts_the_footer_contract_for_every_release_platform(self) -> None:
        for platform in metadata.SUPPORTED_PLATFORMS:
            with self.subTest(platform=platform):
                self.assertEqual(
                    metadata.validate_footer(metadata_footer(platform), platform, "v0.4.1"),
                    {
                        "abi": "C_STRUCT_UNSTABLE",
                        "extension_version": "0.4.1",
                        "duckdb_version": "v1.5.5",
                        "platform": platform,
                        "header_marker": "4",
                    },
                )

    def test_rejects_footer_contract_mismatches(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "platform"):
            metadata.validate_footer(metadata_footer("linux_amd64"), "osx_arm64", "v0.4.1")
        footer = metadata_footer("linux_amd64")
        footer[metadata.HEADER_MARKER_OFFSET] = ord("3")
        with self.assertRaisesRegex(RuntimeError, "header_marker"):
            metadata.validate_footer(footer, "linux_amd64", "v0.4.1")

    def test_rejects_unknown_platform(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "unsupported release platform"):
            metadata.validate_footer(metadata_footer("linux_amd64"), "linux_ppc64le", "v0.4.1")

    def test_rejects_a_truncated_footer(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "footer must be 512 bytes"):
            metadata.validate_footer(metadata_footer("linux_amd64")[:-1], "linux_amd64", "v0.4.1")


if __name__ == "__main__":
    unittest.main()
