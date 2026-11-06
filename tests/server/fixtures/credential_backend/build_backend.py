"""Dependency-free wheel builder for the installed test plugin (PEP 517)."""

import base64
import csv
import hashlib
import io
import zipfile
from pathlib import Path

import tomllib


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    source = Path(__file__).parent
    project = tomllib.loads((source / "pyproject.toml").read_text())["project"]
    name = project["name"].replace("-", "_")
    version = project["version"]
    dist_info = f"{name}-{version}.dist-info"
    files = {
        "fixture_credentials.py": (source / "fixture_credentials.py").read_bytes(),
        f"{dist_info}/METADATA": (
            f"Metadata-Version: 2.3\nName: {project['name']}\nVersion: {version}\n"
        ).encode(),
        f"{dist_info}/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: credential-test-fixture\n"
            b"Root-Is-Purelib: true\nTag: py3-none-any\n"
        ),
        f"{dist_info}/entry_points.txt": "\n".join(
            f"[{group}]\n" + "\n".join(f"{key} = {value}" for key, value in entries.items())
            for group, entries in project["entry-points"].items()
        ).encode(),
    }
    record = io.StringIO(newline="")
    writer = csv.writer(record)
    for path, data in files.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
        writer.writerow([path, f"sha256={digest}", len(data)])
    writer.writerow([f"{dist_info}/RECORD", "", ""])
    files[f"{dist_info}/RECORD"] = record.getvalue().encode()
    filename = f"{name}-{version}-py3-none-any.whl"
    with zipfile.ZipFile(Path(wheel_directory) / filename, "w") as archive:
        for path, data in files.items():
            archive.writestr(path, data)
    return filename
