#!/usr/bin/env python3
"""Create and verify the architecture-independent Encrypted Spaces guest bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
from pathlib import Path


VERSION = 1
GUEST_DIR_ENV = "ENCRYPTED_SPACES_PREBUILT_GUEST_DIR"
EXPECTED_METHODS = {
    "encrypted-spaces-ffproof-methods": ("EXTEND_FF", "HASH_TEST"),
    "encrypted-spaces-client-methods": ("CHECK_ENTRY", "CHECK_NONCE"),
    "ffproof-tracer-methods": ("BENCH_TRACER",),
}
METHOD_RE = re.compile(
    r"pub const (?P<name>[A-Z0-9_]+)_ELF: &\[u8\] = "
    r"include_bytes!\((?P<path>\"(?:\\.|[^\"\\])*\")\);\s*"
    r"pub const (?P=name)_PATH: &str = .*?;\s*"
    r"pub const (?P=name)_ID: \[u32; 8\] = \[(?P<id>[^]]+)\];",
    re.DOTALL,
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def generated_methods(target_dir: Path, package: str) -> Path:
    matches = sorted(target_dir.glob(f"release/build/{package}-*/out/methods.rs"))
    if len(matches) != 1:
        raise SystemExit(
            f"expected one generated methods.rs for {package}, found {len(matches)}"
        )
    return matches[0]


def parse_methods(source: Path, package: str) -> list[dict[str, object]]:
    parsed: dict[str, dict[str, object]] = {}
    for match in METHOD_RE.finditer(source.read_text()):
        name = match.group("name")
        elf = Path(json.loads(match.group("path")))
        words = [int(word.strip()) for word in match.group("id").split(",") if word.strip()]
        if len(words) != 8 or any(word < 0 or word > 0xFFFFFFFF for word in words):
            raise SystemExit(f"invalid guest image ID for {package}/{name}")
        if not any(words):
            raise SystemExit(f"zero guest image ID for {package}/{name}")
        if elf.suffix != ".bin" or not elf.is_file() or elf.stat().st_size == 0:
            raise SystemExit(f"missing guest ELF for {package}/{name}: {elf}")
        if name in parsed:
            raise SystemExit(f"duplicate guest method for {package}/{name}")
        parsed[name] = {"name": name, "source": elf, "image_id": words}
    if set(parsed) != set(EXPECTED_METHODS[package]):
        raise SystemExit(f"unexpected methods for {package}: {tuple(parsed)}")
    return [parsed[name] for name in EXPECTED_METHODS[package]]


def normalized_method(name: str, package: str, image_id: list[int]) -> str:
    filename = f"{name.lower()}.bin"
    relative = f"/{package}/{filename}"
    words = ", ".join(str(word) for word in image_id)
    return (
        f'pub const {name}_ELF: &[u8] = include_bytes!(concat!('
        f'env!("{GUEST_DIR_ENV}"), "{relative}"));\n'
        f'pub const {name}_PATH: &str = concat!('
        f'env!("{GUEST_DIR_ENV}"), "{relative}");\n'
        f"pub const {name}_ID: [u32; 8] = [{words}];\n"
    )


def write_checksums(bundle_dir: Path) -> None:
    files = sorted(path for path in bundle_dir.rglob("*") if path.is_file())
    lines = [f"{sha256(path)}  {path.relative_to(bundle_dir).as_posix()}" for path in files]
    (bundle_dir / "SHA256SUMS").write_text("\n".join(lines) + "\n")


def create(target_dir: Path, output_dir: Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    manifest: dict[str, object] = {"version": VERSION, "packages": []}
    package_records: list[dict[str, object]] = []
    for package in EXPECTED_METHODS:
        package_dir = output_dir / package
        package_dir.mkdir()
        method_records: list[dict[str, object]] = []
        normalized = []
        for method in parse_methods(generated_methods(target_dir, package), package):
            name = str(method["name"])
            source = Path(method["source"])
            image_id = list(method["image_id"])
            filename = f"{name.lower()}.bin"
            destination = package_dir / filename
            shutil.copyfile(source, destination)
            normalized.append(normalized_method(name, package, image_id))
            method_records.append(
                {
                    "name": name,
                    "file": f"{package}/{filename}",
                    "image_id": image_id,
                    "sha256": sha256(destination),
                    "size": destination.stat().st_size,
                }
            )
        (package_dir / "methods.rs").write_text("".join(normalized))
        package_records.append({"name": package, "methods": method_records})
    manifest["packages"] = package_records
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    write_checksums(output_dir)


def safe_relative(bundle_dir: Path, relative: str) -> Path:
    candidate = Path(relative)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise SystemExit(f"unsafe checksum path: {relative}")
    path = bundle_dir / candidate
    current = bundle_dir
    for part in candidate.parts:
        current /= part
        if current.is_symlink():
            raise SystemExit(f"symlink is not allowed in guest bundle: {relative}")
    if not path.is_file():
        raise SystemExit(f"checksum path is missing: {relative}")
    try:
        path.resolve(strict=True).relative_to(bundle_dir.resolve(strict=True))
    except ValueError as error:
        raise SystemExit(f"checksum path escapes guest bundle: {relative}") from error
    return path


def verify(bundle_dir: Path) -> None:
    checksum_file = bundle_dir / "SHA256SUMS"
    if checksum_file.is_symlink() or not checksum_file.is_file():
        raise SystemExit("SHA256SUMS is missing")
    checked: set[str] = set()
    for line in checksum_file.read_text().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if not match:
            raise SystemExit(f"invalid SHA256SUMS line: {line}")
        expected, relative = match.groups()
        if relative in checked:
            raise SystemExit(f"duplicate checksum path: {relative}")
        if sha256(safe_relative(bundle_dir, relative)) != expected:
            raise SystemExit(f"checksum mismatch: {relative}")
        checked.add(relative)

    manifest_path = bundle_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("version") != VERSION:
        raise SystemExit("unsupported guest bundle version")
    packages = manifest.get("packages")
    if not isinstance(packages, list) or not all(isinstance(package, dict) for package in packages):
        raise SystemExit("guest bundle packages are missing")
    expected_files = {"manifest.json"}
    if [package.get("name") for package in packages] != list(EXPECTED_METHODS):
        raise SystemExit("guest bundle package set is invalid")
    for package in packages:
        package_name = package["name"]
        methods = package.get("methods")
        if not isinstance(methods, list):
            raise SystemExit(f"methods are missing for {package_name}")
        if tuple(method.get("name") for method in methods) != EXPECTED_METHODS[package_name]:
            raise SystemExit(f"guest method set is invalid for {package_name}")
        methods_relative = f"{package_name}/methods.rs"
        methods_source = safe_relative(bundle_dir, methods_relative)
        expected_files.add(methods_relative)
        source_text = methods_source.read_text()
        expected_source = "".join(
            normalized_method(method["name"], package_name, method["image_id"])
            for method in methods
        )
        if source_text != expected_source:
            raise SystemExit(f"normalized methods are invalid for {package_name}")
        for method in methods:
            image_id = method.get("image_id")
            if (
                not isinstance(image_id, list)
                or len(image_id) != 8
                or any(type(word) is not int or word < 0 or word > 0xFFFFFFFF for word in image_id)
                or not any(image_id)
            ):
                raise SystemExit(f"zero guest image ID for {package_name}/{method.get('name')}")
            relative = method.get("file")
            expected_relative = f"{package_name}/{method['name'].lower()}.bin"
            if not isinstance(relative, str) or relative != expected_relative:
                raise SystemExit("guest method file is invalid")
            path = safe_relative(bundle_dir, relative)
            if sha256(path) != method.get("sha256") or path.stat().st_size != method.get("size"):
                raise SystemExit(f"guest method manifest mismatch: {relative}")
            expected_files.add(relative)
    if checked != expected_files:
        raise SystemExit(
            f"guest checksum file set mismatch: expected {sorted(expected_files)}, got {sorted(checked)}"
        )
    actual_files = {
        path.relative_to(bundle_dir).as_posix()
        for path in bundle_dir.rglob("*")
        if path.is_file() and path != checksum_file
    }
    symlinks = [
        path.relative_to(bundle_dir).as_posix()
        for path in bundle_dir.rglob("*")
        if path.is_symlink()
    ]
    if symlinks:
        raise SystemExit(f"symlinks are not allowed in guest bundle: {sorted(symlinks)}")
    if actual_files != expected_files:
        raise SystemExit(
            f"guest bundle file set mismatch: expected {sorted(expected_files)}, got {sorted(actual_files)}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    create_parser = subparsers.add_parser("create")
    create_parser.add_argument("--target-dir", type=Path, required=True)
    create_parser.add_argument("--output-dir", type=Path, required=True)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--bundle-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "create":
        create(args.target_dir, args.output_dir)
        verify(args.output_dir)
    else:
        verify(args.bundle_dir)


if __name__ == "__main__":
    main()
