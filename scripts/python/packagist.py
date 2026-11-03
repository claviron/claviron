"""Prepare and publish committed PHP snapshots as Packagist distribution repos.

Preparation writes only ignored local Git mirrors. Publication requires --send
and an explicit remote, uses an atomic fast-forward push, and never force-pushes.
No Composer install, package hook, Rust build or registry submission is executed.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tempfile
import tomllib
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[2]
PROJECT = "claviron"


class ReleaseError(Exception):
    """An explicit source, version or destination decision is required."""


def git(directory, *args, data=None, env=None, optional=False):
    result = subprocess.run(
        ["git", "-C", str(directory), *args], input=data,
        capture_output=True, env=env,
    )
    if result.returncode and not optional:
        # A remote error can contain credentials supplied by a Git helper.
        raise ReleaseError(f"Git {args[0]} failed; inspect the repository or remote separately.")
    return result.stdout if result.returncode == 0 else None


def object_id(directory, ref):
    value = git(directory, "rev-parse", "--verify", ref, optional=True)
    return value.decode().strip() if value else None


def validate_tag(tag):
    if not re.fullmatch(r"v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?", tag):
        raise ReleaseError("Use an explicit release tag such as v1.2.3 or v1.2.3-alpha.1.")


def validate_remote(remote):
    if not remote or remote.startswith("-"):
        raise ReleaseError("An explicit destination remote is required.")
    if "://" in remote and (urlsplit(remote).username or urlsplit(remote).password):
        raise ReleaseError("Do not put credentials in remote URLs; use Git's credential helper.")


@contextmanager
def exclusive(directory):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "publisher.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ReleaseError("Another Composer publisher owns this mirror.") from error
        yield


def selected(path, includes):
    return any(path == item or path.startswith(item + "/") for item in includes)


def snapshot(root, package, ref):
    source = source_repository(root, package)
    prefix = package["directory"].strip("/")
    for path in [prefix, package["license"], *package["include"]]:
        if PurePosixPath(path).is_absolute() or ".." in PurePosixPath(path).parts:
            raise ReleaseError("Package export paths must stay inside the source repository.")
    revision = object_id(source, ref + "^{commit}")
    if revision is None:
        raise ReleaseError("The selected source revision is not a commit.")
    paths = [f"{prefix}/{item}" for item in package["include"]] + [package["license"]]
    if revision == object_id(source, "HEAD") and git(
        source, "status", "--porcelain", "--untracked-files=all", "--", *paths,
    ).strip():
        raise ReleaseError("Commit the selected PHP package and license changes before preparing; unrelated changes may remain.")
    files = {}
    listing = git(source, "ls-tree", "-rz", revision, "--", prefix)
    for entry in listing.split(b"\0"):
        if not entry:
            continue
        metadata, full_path = entry.split(b"\t", 1)
        mode, kind, blob = metadata.decode().split()
        path = full_path.decode().removeprefix(prefix + "/")
        if not selected(path, package["include"]):
            continue
        if mode not in ("100644", "100755") or kind != "blob":
            raise ReleaseError("PHP distribution snapshots must contain regular files, not symlinks or submodules.")
        files[path] = (mode, git(source, "cat-file", "blob", blob))
    for required in ("composer.json", "README.md"):
        if required not in files:
            raise ReleaseError(f"The committed package is missing {required}.")
    metadata = json.loads(files["composer.json"][1])
    if metadata["name"] != package["name"] or "repositories" in metadata:
        raise ReleaseError("Package identity mismatch or package-local repository override.")
    # Source manifests may keep a version for existing local path consumers.
    # Packagist derives distribution versions exclusively from release tags.
    metadata.pop("version", None)
    files["composer.json"] = ("100644", (json.dumps(metadata, indent=2, ensure_ascii=False) + "\n").encode())
    files["LICENSE"] = ("100644", git(source, "show", revision + ":" + package["license"]))
    for section in ("autoload", "autoload-dev"):
        autoload = metadata.get(section, {})
        declared = list(autoload.get("files", []))
        for value in autoload.get("psr-4", {}).values():
            declared.extend(value if isinstance(value, list) else [value])
        for path in declared:
            if not any(name == path or name.startswith(path.rstrip("/") + "/") for name in files):
                raise ReleaseError(f"An autoload path is absent from the export: {path}")
    return source, revision, files


def source_repository(root, package):
    if "repository" in package:
        raise ReleaseError("Composer sources must belong to this repository; repository overrides are not supported.")
    return root.resolve()


def mirror_path(root, config, package):
    return root / config["state-directory"] / package["name"].replace("/", "-")


def parent_commit(mirror, remote=None):
    local = object_id(mirror, "refs/heads/main")
    if not remote:
        return local
    validate_remote(remote)
    heads = git(mirror, "ls-remote", "--heads", remote, "refs/heads/main")
    if not heads.strip():
        return local
    git(mirror, "fetch", "--no-tags", remote, "refs/heads/main:refs/remotes/distribution/main")
    upstream = object_id(mirror, "refs/remotes/distribution/main")
    if local is None or git(mirror, "merge-base", "--is-ancestor", local, upstream, optional=True) is not None:
        return upstream
    if git(mirror, "merge-base", "--is-ancestor", upstream, local, optional=True) is not None:
        return local
    raise ReleaseError("Distribution history diverged; no reset or force-push was attempted.")


def prepare(root, config, package, tag, ref="HEAD", remote=None):
    validate_tag(tag)
    source, revision, files = snapshot(root, package, ref)
    directory = mirror_path(root, config, package)
    with exclusive(directory):
        mirror = directory / "mirror.git"
        if not mirror.exists():
            git(directory, "init", "--bare", str(mirror))
        # Reuse the source repository's identity, including local Git config.
        identity = dict(os.environ)
        for role in ("AUTHOR", "COMMITTER"):
            value = git(source, "var", f"GIT_{role}_IDENT").decode().strip()
            match = re.fullmatch(r"(.+) <([^>]+)> ([0-9]+ [+-][0-9]{4})", value)
            if not match:
                raise ReleaseError("Git did not provide a valid publication identity.")
            identity.update({f"GIT_{role}_NAME": match[1], f"GIT_{role}_EMAIL": match[2], f"GIT_{role}_DATE": match[3]})
        parent = parent_commit(mirror, remote)
        with tempfile.TemporaryDirectory(prefix="composer-index-") as temporary:
            env = dict(os.environ, GIT_INDEX_FILE=str(Path(temporary) / "index"))
            git(mirror, "read-tree", "--empty", env=env)
            entries = []
            for path, (mode, content) in sorted(files.items()):
                blob = git(mirror, "hash-object", "-w", "--stdin", data=content).strip()
                entries.append(mode.encode() + b" " + blob + b"\t" + path.encode() + b"\0")
            git(mirror, "update-index", "-z", "--index-info", data=b"".join(entries), env=env)
            tree = git(mirror, "write-tree", env=env).decode().strip()
        tag_ref = "refs/tags/" + tag
        existing = object_id(mirror, tag_ref + "^{commit}")
        if existing:
            if object_id(mirror, existing + "^{tree}") != tree:
                raise ReleaseError("This release tag already contains different bytes; select a new version.")
            return mirror, existing
        message = f"Release {package['name']} {tag}\n\nSource-Revision: {revision}\nSource-Directory: {package['directory']}\n"
        args = ["commit-tree", tree]
        if parent:
            args.extend(["-p", parent])
        commit = git(mirror, *args, data=message.encode(), env=identity).decode().strip()
        git(mirror, "update-ref", tag_ref, commit, "0" * 40)
        git(mirror, "update-ref", "refs/heads/main", commit)
        return mirror, commit


def publish(root, config, package, tag, remote, send=False):
    validate_tag(tag)
    validate_remote(remote)
    directory = mirror_path(root, config, package)
    mirror = directory / "mirror.git"
    if not mirror.is_dir():
        raise ReleaseError("Prepare this package release before publishing.")
    with exclusive(directory):
        commit = object_id(mirror, "refs/tags/" + tag + "^{commit}")
        if not commit:
            raise ReleaseError("The selected release tag has not been prepared.")
        if not send:
            print(f"Prepared {package['name']} {tag} ({commit}); no remote request or push. Add --send to publish.")
            return
        remote_tag = git(mirror, "ls-remote", "--tags", remote, "refs/tags/" + tag).decode().split()
        if remote_tag:
            if remote_tag[0] != commit:
                raise ReleaseError("The remote release tag already exists with another commit.")
            print(f"Already published: {package['name']} {tag}")
            return
        # Fetch only to prove fast-forward ancestry; never rebuild prepared bytes.
        parent_commit(mirror, remote)
        git(mirror, "push", "--atomic", remote,
            f"{commit}:refs/heads/main", f"refs/tags/{tag}:refs/tags/{tag}")
        print(f"Published distribution: {package['name']} {tag}. Packagist submission/hook setup is separate.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "prepare", "publish"))
    parser.add_argument("--package", required=True)
    parser.add_argument("--tag")
    parser.add_argument("--ref", default="HEAD", help="Committed source revision for prepare")
    parser.add_argument("--remote", help="Override the configured distribution repository URL; prepare only reads it")
    parser.add_argument("--send", action="store_true", help="Actually push a prepared distribution")
    args = parser.parse_args(argv)
    try:
        config = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"][PROJECT]["composer"]
        package = next((p for p in config["packages"] if p["name"] == args.package), None)
        if package is None:
            raise ReleaseError("Unknown configured Composer package.")
        source = source_repository(ROOT, package)
        remote = args.remote or package.get("remote")
        if remote:
            validate_remote(remote)
        if args.send and args.command != "publish":
            raise ReleaseError("--send is only valid with publish.")
        if args.command == "plan":
            manifest = json.loads((source / package["directory"] / "composer.json").read_text())
            print(f"{package['name']}: {source / package['directory']}")
            print("Export: " + ", ".join(package["include"]) + ", LICENSE")
            print("Dependencies: " + ", ".join(manifest.get("require", {})))
            print("Distribution: " + (remote or "not configured; supply --remote"))
        elif not args.tag:
            raise ReleaseError("An explicit --tag is required; no Rust or PHP version is inferred.")
        elif args.command == "prepare":
            mirror, commit = prepare(ROOT, config, package, args.tag, args.ref, remote)
            print(f"Prepared {package['name']} {args.tag}: {commit}\nLocal mirror: {mirror}")
        elif not remote:
            raise ReleaseError("Configure a distribution repository or supply --remote.")
        else:
            publish(ROOT, config, package, args.tag, remote, args.send)
    except (ReleaseError, OSError, ValueError, KeyError) as error:
        print(f"Composer publisher stopped: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
