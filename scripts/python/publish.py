"""Prepare once with Cargo, then upload immutable artifacts to crates.io.

No Rust build, packaging, or upload runs unless its subcommand is selected.
The upload envelope follows Cargo's documented Registry Web API.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import struct
import subprocess
import sys
import tarfile
import time
import tomllib
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener


INDEX = "https://index.crates.io/"
UPLOAD = "https://crates.io/api/v1/crates/new"
USER_AGENT = "claviron-publisher (https://github.com/claviron/claviron)"
ROOT = Path(__file__).resolve().parents[2]


class PublishError(Exception):
    """A failure that requires an operator decision, not a blind retry."""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never forward publication credentials or archives to another URL.
        return None


def run(root, *args):
    result = subprocess.run(args, cwd=root, text=True, capture_output=True)
    if result.returncode:
        raise PublishError(result.stderr.strip() or f"{args[0]} failed")
    return result.stdout


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write_json(path, data):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as stream:
        json.dump(data, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fingerprint(root):
    """Require a fixed clean checkout for preparation, including its Cargo lockfile."""
    if run(root, "git", "status", "--porcelain", "--untracked-files=all").strip():
        raise PublishError("Commit or remove working-tree changes before preparing.")
    revision = run(root, "git", "rev-parse", "HEAD").strip()
    lock = root / "Cargo.lock"
    return {
        "revision": revision,
        "lock": digest(lock.read_bytes()) if lock.exists() else None,
        "cargo": run(root, "cargo", "--version").strip(),
    }


def packages_from(metadata, version):
    members = set(metadata["workspace_members"])
    packages = {
        p["name"]: p for p in metadata["packages"]
        if p["id"] in members and p["publish"] != []
    }
    for name, package in packages.items():
        if package["version"] != version:
            raise PublishError(f"{name} does not use workspace version {version}.")
        if package["publish"] is not None and "crates-io" not in package["publish"]:
            raise PublishError(f"{name} does not allow crates.io publication.")
    return packages


def dependency_order(packages):
    paths = {str(Path(p["manifest_path"]).parent): name for name, p in packages.items()}
    edges = {}
    for name, package in packages.items():
        dependencies = set()
        for dep in package["dependencies"]:
            if dep.get("path"):
                other = paths.get(str(Path(dep["path"])))
                if other is None:
                    # Cargo normalizes versioned sibling paths to registry
                    # dependencies. They are prerequisites, not release members;
                    # Cargo package still verifies the actual registry source.
                    if dep.get("registry") or dep.get("req", "*") == "*":
                        raise PublishError(f"{name}: path dependency {dep['name']} needs a crates.io version requirement outside the release.")
                    continue
                dependencies.add(other)
            elif dep.get("registry") or (dep.get("source") or "").startswith("git+"):
                raise PublishError(f"{name}: {dep['name']} is not a crates.io dependency.")
        edges[name] = dependencies
    return publication_order(edges)


def publication_order(edges):
    # Publish the facade last, including siblings it does not directly depend on.
    if "claviron" in edges:
        edges["claviron"].update(set(edges) - {"claviron"})
    order = []
    remaining = {name: set(deps) for name, deps in edges.items()}
    while remaining:
        ready = sorted(name for name, deps in remaining.items() if not deps)
        if not ready:
            raise PublishError("Workspace publication dependency cycle: " + ", ".join(remaining))
        order.extend(ready)
        for name in ready:
            del remaining[name]
        for deps in remaining.values():
            deps.difference_update(ready)
    return order


def index_path(name):
    name = name.lower()
    if len(name) < 3:
        return f"{len(name)}/{name}"
    if len(name) == 3:
        return f"3/{name[0]}/{name}"
    return f"{name[:2]}/{name[2:4]}/{name}"


def retry_delay(headers, body, now):
    """Only the server may choose the next attempt time."""
    value = headers.get("Retry-After")
    if value and value.isdigit():
        delay = float(value)
    else:
        if not value:
            match = re.search(r"try again after (.+? GMT)", body, re.IGNORECASE)
            value = match[1] if match else None
        if not value:
            raise PublishError("Rate limited without a retry time; stopped without guessing a backoff.")
        try:
            deadline = parsedate_to_datetime(value).timestamp()
            # Prefer the server's clock when it supplied a Date header.
            baseline = parsedate_to_datetime(headers["Date"]).timestamp() if headers.get("Date") else now
            delay = deadline - baseline
        except (ValueError, TypeError, OverflowError) as error:
            raise PublishError("Registry returned an invalid retry time.") from error
    if delay <= 0:
        raise PublishError("Registry retry time has already elapsed; stopped instead of looping.")
    return delay


class Registry:
    def __init__(self, timeout=None):
        self.timeout = timeout
        self.opener = build_opener(NoRedirect())

    def request(self, url, data=None, token=None, etag=None):
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        if etag:
            headers["If-None-Match"] = etag
        if token:
            headers.update({"Authorization": token, "Content-Type": "application/octet-stream"})
        request = Request(url, data=data, headers=headers, method="PUT" if data is not None else "GET")
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                return response.status, response.headers, response.read()
        except HTTPError as error:
            return error.code, error.headers, error.read()
        except (URLError, OSError) as error:
            # Do not display exception/request internals: they may contain credentials.
            raise PublishError("Registry connection failed; upload result may be unknown. No automatic resend.") from error


def archive_metadata(path, package):
    """Read Cargo's normalized manifest and README without extracting the tarball."""
    prefix = f"{package['name']}-{package['version']}/"
    with tarfile.open(path, "r:gz") as archive:
        def read(name):
            try:
                member = archive.getmember(prefix + name)
            except KeyError as error:
                raise PublishError(f"{path.name}: missing {name}") from error
            if not member.isfile():
                raise PublishError(f"{path.name}: {name} is not a regular file")
            return archive.extractfile(member).read().decode("utf-8")

        manifest = tomllib.loads(read("Cargo.toml"))
        info = manifest["package"]
        if (info["name"], info["version"]) != (package["name"], package["version"]):
            raise PublishError(f"Archive identity mismatch: {path}")
        deps = []

        def dependencies(table, target=None):
            for section, kind in (("dependencies", "normal"), ("build-dependencies", "build"), ("dev-dependencies", "dev")):
                for alias, dep in table.get(section, {}).items():
                    if isinstance(dep, str):
                        dep = {"version": dep}
                    if any(key in dep for key in ("git", "path", "registry", "registry-index", "artifact")):
                        raise PublishError(f"Unsupported packaged dependency in {path.name}: {alias}")
                    if not dep.get("version"):
                        raise PublishError(f"Missing packaged dependency version: {alias}")
                    deps.append({
                        "name": dep.get("package", alias), "version_req": dep["version"],
                        "features": dep.get("features", []), "optional": dep.get("optional", False),
                        "default_features": dep.get("default-features", True),
                        "target": target, "kind": kind, "registry": None,
                        "explicit_name_in_toml": alias if dep.get("package") else None,
                    })

        dependencies(manifest)
        for target, table in manifest.get("target", {}).items():
            dependencies(table, target)
        readme = info.get("readme")
        payload = {key: info.get(key.replace("_", "-")) for key in (
            "description", "documentation", "homepage", "license", "license_file", "repository", "links", "rust_version",
        )}
        payload.update({
            "name": info["name"], "vers": info["version"], "deps": deps,
            # Cargo metadata includes the implicit optional-dependency features.
            "features": package["features"], "authors": info.get("authors", []),
            "keywords": info.get("keywords", []), "categories": info.get("categories", []),
            "readme": read(readme) if isinstance(readme, str) else None,
            "readme_file": readme if isinstance(readme, str) else None,
            "badges": manifest.get("badges", {}),
        })
        return payload


def upload_body(metadata, archive):
    encoded = json.dumps(metadata, separators=(",", ":"), ensure_ascii=False).encode()
    # The Cargo registry protocol defines both lengths as unsigned little-endian u32.
    return struct.pack("<I", len(encoded)) + encoded + struct.pack("<I", len(archive)) + archive


def validate_cached_archive(path, package, listed_files, root):
    """Adopt Cargo output only when its published inputs still match the checkout."""
    payload = archive_metadata(path, package)
    prefix = f"{package['name']}-{package['version']}/"
    package_root = Path(package["manifest_path"]).parent
    generated = {"Cargo.toml", "Cargo.lock", ".cargo_vcs_info.json"}
    with tarfile.open(path, "r:gz") as archive:
        files = {m.name.removeprefix(prefix): m for m in archive.getmembers() if m.isfile() and m.name.startswith(prefix)}
        if set(files) != set(listed_files):
            raise PublishError(f"{path.name}: cached package file list no longer matches the checkout.")
        vcs = json.loads(archive.extractfile(files[".cargo_vcs_info.json"]).read())
        if vcs["git"].get("dirty") or vcs["path_in_vcs"] != package_root.relative_to(root).as_posix().removeprefix("."):
            raise PublishError(f"{path.name}: cached package has dirty or mismatched source provenance.")
        for name, member in files.items():
            if name in generated:
                continue
            source = package_root / ("Cargo.toml" if name == "Cargo.toml.orig" else name)
            # Cargo may copy workspace-level license files into a child package.
            if not source.is_file() and name in {"LICENSE-MIT", "LICENSE-APACHE"}:
                source = root / name
            if not source.is_file() or source.read_bytes() != archive.extractfile(member).read():
                raise PublishError(f"{path.name}: cached source differs: {name}")

    def requirement(value):
        # Cargo metadata makes the implicit caret explicit; normalized manifests do not.
        return value.removeprefix("^")

    expected = [{
        "name": dep["name"], "version_req": requirement(dep["req"]),
        "features": sorted(dep["features"]), "optional": dep["optional"],
        "default_features": dep["uses_default_features"], "target": dep["target"],
        "kind": dep["kind"] or "normal", "registry": None,
        "explicit_name_in_toml": dep.get("rename"),
    } for dep in package["dependencies"]]
    actual = [dict(dep, version_req=requirement(dep["version_req"]), features=sorted(dep["features"])) for dep in payload["deps"]]
    encode = lambda dep: json.dumps(dep, sort_keys=True)
    if sorted(map(encode, actual)) != sorted(map(encode, expected)):
        raise PublishError(f"{path.name}: cached dependencies no longer match the workspace.")
    return payload


def cargo_token():
    token = os.environ.get("CARGO_REGISTRY_TOKEN")
    if not token:
        cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
        for filename in ("credentials.toml", "credentials"):
            path = cargo_home / filename
            if path.exists():
                token = tomllib.loads(path.read_text()).get("registry", {}).get("token")
                if token:
                    break
    if not isinstance(token, str) or not token or any(ord(c) < 32 or ord(c) > 126 for c in token):
        raise PublishError("Set CARGO_REGISTRY_TOKEN or use cargo login. Custom credential providers are not executed.")
    return token


class Publisher:
    def __init__(self, root, directory, registry):
        self.root = root
        self.directory = directory
        self.registry = registry
        self.path = directory / "state.json"
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {"schema": 1, "releases": {}, "indices": {}}
        if self.state.get("schema") != 1:
            raise PublishError("Unsupported publisher state schema.")
        self.indices = {}

    def save(self):
        write_json(self.path, self.state)

    def entry(self, name, version):
        return self.state["releases"].setdefault(f"{name}@{version}", {})

    def prepared_selection(self, names, version):
        """Select recorded artifacts, including dependencies retired from the checkout."""
        packages, edges = {}, {}

        def include(name):
            if name in packages:
                return
            entry = self.state["releases"].get(f"{name}@{version}")
            if not entry or not (entry.get("metadata") or entry.get("published")):
                raise PublishError(f"{name}@{version}: no prepared release receipt. Nothing uploaded.")
            if not entry.get("published"):
                self.artifact(name, version)
            packages[name] = {"name": name, "version": version}
            edges[name] = set()
            for dependency in entry.get("metadata", {}).get("deps", []):
                other = dependency["name"]
                # Recorded members of the same release are ordered even when optional.
                if f"{other}@{version}" in self.state["releases"]:
                    include(other)
                    edges[name].add(other)

        for name in names:
            include(name)
        return packages, publication_order(edges)

    def published(self, name, version, refresh=False):
        entry = self.entry(name, version)
        # Exact versions are immutable. Persist positive results, never negative ones.
        if entry.get("published") and not refresh:
            return True
        if name not in self.indices:
            cached = self.state.setdefault("indices", {}).get(name, {})
            status, headers, body = self.registry.request(INDEX + index_path(name), etag=cached.get("etag"))
            if status == 404:
                self.indices[name] = []
            elif status == 200:
                self.indices[name] = [json.loads(line) for line in body.splitlines() if line]
                self.state["indices"][name] = {"etag": headers.get("ETag"), "records": self.indices[name]}
                self.save()
            elif status == 304 and "records" in cached:
                self.indices[name] = cached["records"]
            else:
                raise PublishError(f"Index lookup failed for {name}: HTTP {status}. Nothing uploaded.")
        record = next((r for r in self.indices[name] if r["vers"] == version), None)
        if record is None:
            if entry.get("published"):
                raise PublishError(f"Previously published {name}@{version} disappeared from the index.")
            return False
        if record.get("yanked"):
            raise PublishError(f"{name}@{version} is yanked; do not republish it.")
        if entry.get("sha256") and entry["sha256"] != record["cksum"]:
            raise PublishError(f"Published checksum differs from prepared {name}@{version}.")
        entry["published"] = record["cksum"]
        entry.pop("attempt", None)
        entry.pop("retry_at", None)
        self.save()
        return True

    def prepare(self, packages, order, metadata, no_verify):
        source = fingerprint(self.root)
        missing = []
        for name in order:
            package = packages[name]
            entry = self.entry(name, package["version"])
            if entry.get("published"):
                continue
            ready = entry.get("source") == source and (no_verify or entry.get("verified"))
            artifact = self.directory / f"{name}-{package['version']}.crate"
            if ready and artifact.exists() and digest(artifact.read_bytes()) == entry.get("sha256"):
                print(f"Ready: {name}@{package['version']} (reused)", flush=True)
            else:
                if entry.get("attempt"):
                    raise PublishError(f"{name}: resolve the previous {entry['attempt']} upload before replacing its artifact.")
                if no_verify:
                    package_directory = Path(metadata["target_directory"]) / "package"
                    candidates = [package_directory / artifact.name, package_directory / "tmp-registry" / artifact.name]
                    existing = next((path for path in candidates if path.is_file()), None)
                    if existing:
                        listed = run(self.root, "cargo", "package", "--list", "--offline", "--locked", "--allow-dirty", "--registry", "crates-io", "-p", name).splitlines()
                        try:
                            payload = validate_cached_archive(existing, package, listed, self.root)
                        except PublishError as error:
                            print(f"Cache not reusable: {error}", flush=True)
                        else:
                            if fingerprint(self.root) != source:
                                raise PublishError("Source changed while importing existing packages.")
                            self.record_artifact(existing, package, source, payload, verified=False)
                            print(f"Prepared: {name}@{package['version']} (existing Cargo archive)", flush=True)
                            continue
                missing.append(name)
        if not missing:
            return
        command = ["cargo", "package", "--workspace", "--registry", "crates-io", "--locked"]
        if no_verify:
            command.append("--no-verify")
        workspace = {p["name"] for p in metadata["packages"] if p["id"] in metadata["workspace_members"]}
        for name in sorted(workspace - set(missing)):
            command.extend(["--exclude", name])
        # One workspace packaging pass, not a Cargo invocation per crate/retry.
        result = subprocess.run(command, cwd=self.root)
        if result.returncode:
            raise PublishError("Cargo packaging failed. No uploads were attempted.")
        if fingerprint(self.root) != source:
            raise PublishError("Source changed during preparation; no readiness receipts were recorded.")
        for name in missing:
            package = packages[name]
            package_directory = Path(metadata["target_directory"]) / "package"
            filename = f"{name}-{package['version']}.crate"
            artifact = package_directory / filename
            if not artifact.exists():
                artifact = package_directory / "tmp-registry" / filename
            payload = archive_metadata(artifact, package)
            self.record_artifact(artifact, package, source, payload, verified=not no_verify)
            print(f"Prepared: {name}@{package['version']}", flush=True)

    def record_artifact(self, artifact, package, source, payload, verified):
        content = artifact.read_bytes()
        destination = self.directory / artifact.name
        temporary = destination.with_suffix(".tmp")
        temporary.write_bytes(content)
        temporary.replace(destination)
        self.entry(package["name"], package["version"]).update({
            "source": source, "sha256": digest(content), "metadata": payload, "verified": verified,
            "metadata_sha256": digest(json.dumps(payload, sort_keys=True).encode()),
        })
        self.save()

    def publish(self, packages, order):
        # Publication uses the prepared bytes and metadata, not the current checkout.
        # Preflight every artifact before sending the first one.
        for name in order:
            package = packages[name]
            entry = self.entry(name, package["version"])
            if entry.get("published"):
                continue
            self.artifact(name, package["version"])
            if entry.get("attempt"):
                raise PublishError(f"{name}: previous upload is {entry['attempt']} but not indexed. No automatic resend; reconcile with crates.io first.")
        token = None
        for name in order:
            version = packages[name]["version"]
            entry = self.entry(name, version)
            if entry.get("published"):
                continue
            token = token or cargo_token()
            while True:
                retry_at = entry.get("retry_at")
                if retry_at and retry_at > time.time():
                    remaining = retry_at - time.time()
                    print(f"Rate limited: waiting until {datetime.fromtimestamp(retry_at, timezone.utc).isoformat()} ({remaining:.0f}s)", flush=True)
                    time.sleep(remaining)
                content = self.artifact(name, version)
                entry["attempt"] = "uncertain"
                self.save()  # Durable before the network write, including Ctrl-C/crash.
                print(f"Uploading: {name}@{version}", flush=True)
                status, headers, body = self.registry.request(UPLOAD, upload_body(entry["metadata"], content), token)
                if status == 429:
                    entry.pop("attempt", None)
                    self.save()  # A 429 is an explicit rejection, even without a usable retry time.
                    delay = retry_delay(headers, body.decode("utf-8", errors="replace"), time.time())
                    entry["retry_at"] = time.time() + delay
                    self.save()
                    continue
                if not 200 <= status < 300:
                    # A 4xx explicitly rejected the operation; 5xx remains uncertain.
                    if 400 <= status < 500:
                        entry.pop("attempt", None)
                        self.save()
                    try:
                        errors = json.loads(body).get("errors", [])
                        details = "; ".join(error["detail"] for error in errors if isinstance(error.get("detail"), str))
                    except (ValueError, TypeError, AttributeError, KeyError):
                        details = ""
                    # Only display protocol error messages, never request credentials or raw HTML.
                    details = details.replace(token, "[redacted]")
                    detail = f" {details}" if details else ""
                    raise PublishError(f"Upload failed for {name}: HTTP {status}.{detail} Stopped; no automatic resend.")
                try:
                    response = json.loads(body or b"{}")
                except ValueError as error:
                    raise PublishError(f"{name}: upload response was not valid JSON; reconcile before retrying.") from error
                if response.get("errors"):
                    raise PublishError(f"{name}: registry returned errors; reconcile before retrying.")
                entry["attempt"] = "accepted"
                entry.pop("retry_at", None)
                self.save()
                warnings = {kind: messages for kind, messages in (response.get("warnings") or {}).items() if messages}
                if warnings:
                    print(f"Registry warnings for {name}: {json.dumps(warnings)}", flush=True)
                # Only re-fetch the crate just uploaded. Cached unrelated crates stay untouched.
                self.indices.pop(name, None)
                if not self.published(name, version):
                    raise PublishError(f"{name}: upload accepted, index not visible yet. Rerun publish later; it will check, not re-upload.")
                print(f"Published: {name}@{version}", flush=True)
                break

    def artifact(self, name, version):
        entry = self.entry(name, version)
        path = self.directory / f"{name}-{version}.crate"
        if not entry.get("source") or not path.exists() or not entry.get("metadata"):
            raise PublishError(f"{name}@{version}: run prepare first.")
        content = path.read_bytes()
        if digest(content) != entry.get("sha256"):
            raise PublishError(f"{path.name}: artifact checksum changed. Nothing resent.")
        if digest(json.dumps(entry["metadata"], sort_keys=True).encode()) != entry.get("metadata_sha256"):
            raise PublishError(f"{path.name}: publication metadata checksum changed. Nothing resent.")
        return content


@contextmanager
def exclusive(directory):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "publisher.lock").open("w") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise PublishError("Another publisher is using this release directory.") from error
        yield


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "prepare", "publish"))
    parser.add_argument("--refresh", action="store_true", help="Recheck even known published immutable versions")
    parser.add_argument("--no-verify", action="store_true", help="Prepare only: skip Cargo build verification explicitly")
    parser.add_argument("--send", action="store_true", help="Publish only: authorize actual registry uploads")
    parser.add_argument("--package", action="append", help="Plan/publish only: select a prepared package and its recorded release dependencies; may be repeated")
    parser.add_argument("--request-timeout", type=float, help="Explicit network timeout in seconds; otherwise wait without a deadline")
    args = parser.parse_args(argv)
    if args.no_verify and args.command != "prepare":
        parser.error("--no-verify is only valid with prepare")
    if args.send and args.command != "publish":
        parser.error("--send is only valid with publish")
    if args.package and args.command == "prepare":
        parser.error("--package selects existing artifacts only; use it with plan or publish")
    if args.command == "publish" and not args.send:
        parser.error("publish requires --send; use plan for a read-only registry check")
    if args.request_timeout is not None and args.request_timeout <= 0:
        parser.error("--request-timeout must be positive")
    try:
        project = tomllib.loads((ROOT / "pyproject.toml").read_text())
        config = project["tool"]["claviron"]["publish"]
        directory = ROOT / config["state-directory"]
        version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
        with exclusive(directory):
            publisher = Publisher(ROOT, directory, Registry(args.request_timeout))
            if args.package:
                packages, order = publisher.prepared_selection(args.package, version)
            else:
                metadata = json.loads(run(ROOT, "cargo", "metadata", "--no-deps", "--offline", "--format-version", "1"))
                packages = packages_from(metadata, version)
                order = dependency_order(packages)
            for index, name in enumerate(order, 1):
                published = publisher.published(name, version, args.refresh)
                print(f"{index:02d}. {name}@{version}: {'published — skip' if published else 'pending'}", flush=True)
            if args.command == "prepare":
                publisher.prepare(packages, order, metadata, args.no_verify)
            elif args.command == "publish":
                publisher.publish(packages, order)
        return 0
    except (PublishError, OSError, ValueError, KeyError, tarfile.TarError) as error:
        print(f"Publisher stopped: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nPublisher interrupted. Prepared artifacts and upload receipts are retained.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
