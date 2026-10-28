#!/usr/bin/env python3
"""Prepare and verify Cargo source overrides for this Claviron checkout."""

import argparse
import json
from pathlib import Path
import subprocess
import sys


def metadata(manifest, config=None, dependencies=False):
    command = ["cargo"]
    if config is not None:
        command.extend(["--config", str(config)])
    command.extend(["metadata", "--format-version", "1", "--manifest-path", str(manifest)])
    command.extend(["--all-features"] if dependencies else ["--no-deps"])
    return json.loads(subprocess.check_output(command, cwd=str(manifest.parent), encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--verify", type=Path, metavar="MANIFEST",
                        help="Verify the resolved example graph instead of writing overrides")
    args = parser.parse_args()
    root = args.root.resolve()
    config = args.config.resolve()
    workspace = metadata(root / "Cargo.toml")
    members = set(workspace["workspace_members"])
    packages = {package["name"]: package for package in workspace["packages"]
                if package["id"] in members}
    facade = packages["claviron"]

    if args.verify is not None:
        resolved = metadata(args.verify.resolve(), config, dependencies=True)
        mismatches = [package["name"] for package in resolved["packages"]
                      if package["name"] in packages and (
                          package["source"] is not None or
                          Path(package["manifest_path"]).resolve() !=
                          Path(packages[package["name"]]["manifest_path"]).resolve())]
        if not any(package["name"] == "claviron" for package in resolved["packages"]):
            mismatches.append("claviron")
        if mismatches:
            print("Claviron sources need lockfile refresh: " + ", ".join(sorted(set(mismatches))),
                  file=sys.stderr)
            return 1
        return 0

    lines = ["# Generated local build configuration; not a package manifest.", "[patch.crates-io]"]
    for name, package in sorted(packages.items()):
        directory = str(Path(package["manifest_path"]).resolve().parent)
        lines.append("{} = {{ path = {} }}".format(
            json.dumps(name, ensure_ascii=False), json.dumps(directory, ensure_ascii=False)))
    contents = "\n".join(lines) + "\n"
    config.parent.mkdir(parents=True, exist_ok=True)
    if not config.exists() or config.read_text(encoding="utf-8") != contents:
        config.write_text(contents, encoding="utf-8")
    print(facade["version"])
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, subprocess.CalledProcessError, KeyError, ValueError) as error:
        print("Cannot prepare local Claviron sources: {}".format(error), file=sys.stderr)
        sys.exit(2)
