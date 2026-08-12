import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


def test_wheel_includes_required_runtime_resources(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    package_root = tmp_path / "package-src"
    dist_dir = tmp_path / "dist"

    package_root.mkdir()
    dist_dir.mkdir()

    for file_name in ["pyproject.toml", "setup.py", "MANIFEST.in", "README.md", "LICENSE"]:
        shutil.copy2(repo_root / file_name, package_root / file_name)

    shutil.copytree(repo_root / "dbdemos", package_root / "dbdemos")

    synthetic_bundle = package_root / "dbdemos" / "bundles" / "test-packaging"
    synthetic_bundle.mkdir(parents=True)
    (synthetic_bundle / "conf.json").write_text("{}", encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            ".",
            "--no-deps",
            "--wheel-dir",
            str(dist_dir),
        ],
        cwd=package_root,
        check=True,
        capture_output=True,
        text=True,
    )

    wheel_path = next(dist_dir.glob("dbdemos-*.whl"))
    with zipfile.ZipFile(wheel_path) as wheel_zip:
        wheel_files = set(wheel_zip.namelist())

    assert "dbdemos/resources/default_cluster_config.json" in wheel_files
    assert "dbdemos/template/README.html" in wheel_files
    assert "dbdemos/bundles/test-packaging/conf.json" in wheel_files
