import importlib.util
from pathlib import Path
import struct
import unittest


SCRIPT = Path(__file__).with_name("check-release-asset-metadata.py")
SPEC = importlib.util.spec_from_file_location("check_release_asset_metadata", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
metadata = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(metadata)


def pe_image(import_names: list[str]) -> bytearray:
    pe_offset = 0x80
    optional_size = 0xF0
    section_offset = pe_offset + 24 + optional_size
    raw_offset = 0x200
    section_rva = 0x1000
    image = bytearray(0x600)

    image[:2] = b"MZ"
    struct.pack_into("<I", image, 0x3C, pe_offset)
    image[pe_offset : pe_offset + 4] = b"PE\0\0"
    struct.pack_into(
        "<HHIIIHH", image, pe_offset + 4, 0x8664, 1, 0, 0, 0, optional_size, 0
    )

    optional_offset = pe_offset + 24
    struct.pack_into("<H", image, optional_offset, 0x20B)
    struct.pack_into("<I", image, optional_offset + 108, 16)
    descriptor_size = (len(import_names) + 1) * 20
    struct.pack_into(
        "<II", image, optional_offset + 120, section_rva, descriptor_size
    )

    image[section_offset : section_offset + 8] = b".rdata\0\0"
    struct.pack_into(
        "<IIII", image, section_offset + 8, 0x400, section_rva, 0x400, raw_offset
    )

    name_offset = raw_offset + descriptor_size
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
    return image


class PeImportTests(unittest.TestCase):
    def test_accepts_reviewed_windows_system_import(self) -> None:
        imports = metadata.pe_imports(pe_image(["KERNEL32.dll"]))
        self.assertEqual(imports, {"kernel32.dll"})
        metadata.validate_pe_imports(imports)

    def test_rejects_mingw_runtime_case_insensitively(self) -> None:
        imports = metadata.pe_imports(pe_image(["LIBGCC_S_SEH-1.DLL"]))
        with self.assertRaisesRegex(RuntimeError, "MinGW runtimes"):
            metadata.validate_pe_imports(imports)

    def test_rejects_unreviewed_import(self) -> None:
        imports = metadata.pe_imports(pe_image(["unexpected.dll"]))
        with self.assertRaisesRegex(RuntimeError, "unreviewed PE imports"):
            metadata.validate_pe_imports(imports)

    def test_rejects_non_pe_input(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "not a DOS/PE image"):
            metadata.pe_imports(bytearray(64))


if __name__ == "__main__":
    unittest.main()
