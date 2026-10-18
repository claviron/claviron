"""Release safety tests; no Cargo commands, builds, sleeps, or live requests."""

from email.message import Message
import importlib.util
import io
import json
from pathlib import Path
import struct
import tarfile
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("publisher", Path(__file__).parents[1] / "publish.py")
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)


def package(name, deps=()):
    return {
        "name": name, "version": "1.0.0", "features": {},
        "manifest_path": f"/source/{name}/Cargo.toml",
        "dependencies": [{"name": dep, "path": f"/source/{dep}"} for dep in deps],
    }


def headers(**values):
    result = Message()
    for key, value in values.items():
        result[key.replace("_", "-")] = value
    return result


class FakeRegistry:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


class ReleaseTests(unittest.TestCase):
    def test_order_includes_optional_and_target_dependencies_and_facade_last(self):
        packages = {p["name"]: p for p in (
            package("claviron", ["app"]), package("app", ["core"]),
            package("core"), package("extra"),
        )}
        order = publisher.dependency_order(packages)
        self.assertLess(order.index("core"), order.index("app"))
        self.assertEqual(order[-1], "claviron")
        packages["core"]["dependencies"] = [{"name": "app", "path": "/source/app"}]
        with self.assertRaisesRegex(publisher.PublishError, "cycle"):
            publisher.dependency_order(packages)

    def test_retry_uses_server_clock_or_delta_and_never_guesses(self):
        self.assertEqual(publisher.retry_delay(headers(Retry_After="600"), "", 1), 600)
        response = headers(Date="Sun, 18 Oct 2026 15:00:00 GMT")
        body = "Please try again after Sun, 18 Oct 2026 15:10:00 GMT and see the docs"
        self.assertEqual(publisher.retry_delay(response, body, 0), 600)
        with self.assertRaises(publisher.PublishError):
            publisher.retry_delay(headers(), "rate limited", 1)
        with self.assertRaises(publisher.PublishError):
            publisher.retry_delay(headers(Retry_After="0"), "", 1)

    def test_upload_envelope_matches_cargo_protocol(self):
        body = publisher.upload_body({"name": "demo"}, b"archive")
        length = struct.unpack("<I", body[:4])[0]
        self.assertEqual(json.loads(body[4:4 + length]), {"name": "demo"})
        self.assertEqual(struct.unpack("<I", body[4 + length:8 + length])[0], 7)
        self.assertEqual(body[8 + length:], b"archive")

    def test_normalized_archive_rename_target_and_readme(self):
        manifest = b'''[package]
name = "demo"
version = "1.0.0"
description = "Example"
license = "MIT"
readme = "README.md"
[dependencies.renamed]
package = "original"
version = "1"
optional = true
default-features = false
[target.'cfg(unix)'.build-dependencies.helper]
version = "2"
'''
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "demo-1.0.0.crate"
            with tarfile.open(path, "w:gz") as archive:
                for name, content in (("Cargo.toml", manifest), ("README.md", b"Example README")):
                    member = tarfile.TarInfo("demo-1.0.0/" + name)
                    member.size = len(content)
                    archive.addfile(member, io.BytesIO(content))
            result = publisher.archive_metadata(path, package("demo"))
            self.assertEqual(result["readme"], "Example README")
            self.assertEqual(result["deps"][0]["name"], "original")
            self.assertEqual(result["deps"][0]["explicit_name_in_toml"], "renamed")
            self.assertFalse(result["deps"][0]["default_features"])
            self.assertEqual(result["deps"][1]["target"], "cfg(unix)")
            self.assertEqual(result["deps"][1]["kind"], "build")

    @patch.object(publisher, "fingerprint", return_value={"revision": "fixed"})
    def test_existing_cargo_archive_is_adopted_without_packaging(self, *_):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "demo"
            source.mkdir()
            manifest = b'[package]\nname = "demo"\nversion = "1.0.0"\n'
            (source / "Cargo.toml").write_bytes(manifest)
            (source / "lib.rs").write_bytes(b"pub fn example() {}\n")
            target = root / "target"
            cache = target / "package/tmp-registry"
            cache.mkdir(parents=True)
            path = cache / "demo-1.0.0.crate"
            files = {
                "Cargo.toml": manifest, "Cargo.toml.orig": manifest,
                "lib.rs": (source / "lib.rs").read_bytes(),
                ".cargo_vcs_info.json": json.dumps({"git": {"sha1": "older-commit"}, "path_in_vcs": "demo"}).encode(),
            }
            with tarfile.open(path, "w:gz") as archive:
                for name, content in files.items():
                    member = tarfile.TarInfo("demo-1.0.0/" + name)
                    member.size = len(content)
                    archive.addfile(member, io.BytesIO(content))
            info = package("demo")
            info["manifest_path"] = str(source / "Cargo.toml")
            info["id"] = "demo"
            directory = root / "receipts"
            directory.mkdir()
            app = publisher.Publisher(root, directory, FakeRegistry([]))
            metadata = {"target_directory": str(target), "packages": [info], "workspace_members": ["demo"]}
            with patch.object(publisher, "run", return_value="\n".join(files)), patch.object(publisher.subprocess, "run", side_effect=AssertionError("No packaging")):
                app.prepare({"demo": info}, ["demo"], metadata, no_verify=True)
            self.assertEqual(path.read_bytes(), (directory / path.name).read_bytes())
            self.assertFalse(app.entry("demo", "1.0.0")["verified"])
            (source / "lib.rs").write_bytes(b"changed")
            with self.assertRaisesRegex(publisher.PublishError, "cached source differs"):
                publisher.validate_cached_archive(path, info, files, root)
            with self.assertRaisesRegex(publisher.PublishError, "file list"):
                publisher.validate_cached_archive(path, info, list(files) + ["added.rs"], root)

    def test_published_receipts_avoid_network_and_refresh_uses_etag(self):
        record = {"name": "demo", "vers": "1.0.0", "cksum": "abc", "yanked": False}
        registry = FakeRegistry([(200, headers(ETag='"one"'), json.dumps(record).encode()), (304, headers(), b"")])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            app = publisher.Publisher(root, root, registry)
            self.assertTrue(app.published("demo", "1.0.0"))
            resumed = publisher.Publisher(root, root, registry)
            self.assertTrue(resumed.published("demo", "1.0.0"))
            self.assertEqual(len(registry.calls), 1)
            self.assertTrue(resumed.published("demo", "1.0.0", refresh=True))
            self.assertEqual(registry.calls[1][1]["etag"], '"one"')

    def prepare_fake(self, root, registry):
        app = publisher.Publisher(root, root, registry)
        content = b"prepared"
        (root / "demo-1.0.0.crate").write_bytes(content)
        app.entry("demo", "1.0.0").update({
            "source": {"revision": "fixed"}, "sha256": publisher.digest(content),
            "metadata": {"name": "demo", "vers": "1.0.0"}, "verified": False,
        })
        entry = app.entry("demo", "1.0.0")
        entry["metadata_sha256"] = publisher.digest(json.dumps(entry["metadata"], sort_keys=True).encode())
        return app

    @patch.object(publisher, "fingerprint", return_value={"revision": "fixed"})
    @patch.object(publisher, "cargo_token", return_value="not-a-real-token")
    def test_429_resends_same_archive_without_repackaging(self, *_):
        record = {"vers": "1.0.0", "cksum": publisher.digest(b"prepared"), "yanked": False}
        registry = FakeRegistry([
            (429, headers(Retry_After="10"), b""),
            (200, headers(), b"{}"),
            (200, headers(), json.dumps(record).encode()),
        ])
        with tempfile.TemporaryDirectory() as temporary, patch.object(publisher.time, "sleep") as sleep:
            root = Path(temporary)
            app = self.prepare_fake(root, registry)
            with patch.object(publisher.subprocess, "run", side_effect=AssertionError("Cargo must not run")):
                app.publish({"demo": package("demo")}, ["demo"])
            self.assertEqual(registry.calls[0][0][1], registry.calls[1][0][1])
            self.assertEqual(sleep.call_count, 1)
            self.assertTrue(app.entry("demo", "1.0.0")["published"])

    @patch.object(publisher, "fingerprint", side_effect=AssertionError("Publication must not inspect the checkout"))
    @patch.object(publisher, "cargo_token", return_value="not-a-real-token")
    def test_prepared_upload_does_not_depend_on_current_checkout(self, *_):
        record = {"vers": "1.0.0", "cksum": publisher.digest(b"prepared"), "yanked": False}
        registry = FakeRegistry([
            (200, headers(), b"{}"),
            (200, headers(), json.dumps(record).encode()),
        ])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            app = self.prepare_fake(root, registry)
            (root / "Cargo.toml").write_text("Uncommitted changes after preparation")
            (root / "Cargo.lock").write_text("Changed lockfile after preparation")
            with patch.object(publisher.subprocess, "run", side_effect=AssertionError("No Git or Cargo commands")):
                app.publish({"demo": package("demo")}, ["demo"])
            self.assertTrue(app.entry("demo", "1.0.0")["published"])
            self.assertEqual(registry.calls[0][0][1], publisher.upload_body(
                app.entry("demo", "1.0.0")["metadata"], b"prepared",
            ))

    @patch.object(publisher, "fingerprint", return_value={"revision": "fixed"})
    @patch.object(publisher, "cargo_token", return_value="not-a-real-token")
    def test_uncertain_and_accepted_unindexed_uploads_are_never_resent(self, *_):
        for responses, expected in (
            ([publisher.PublishError("connection lost")], "uncertain"),
            ([(200, headers(), b"{}"), (404, headers(), b"")], "accepted"),
        ):
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as temporary:
                registry = FakeRegistry(responses)
                root = Path(temporary)
                app = self.prepare_fake(root, registry)
                with self.assertRaises(publisher.PublishError):
                    app.publish({"demo": package("demo")}, ["demo"])
                self.assertEqual(app.entry("demo", "1.0.0")["attempt"], expected)
                count = len(registry.calls)
                with self.assertRaisesRegex(publisher.PublishError, "No automatic resend"):
                    app.publish({"demo": package("demo")}, ["demo"])
                self.assertEqual(len(registry.calls), count)

    @patch.object(publisher, "fingerprint", return_value={"revision": "fixed"})
    def test_tampered_artifact_blocks_upload(self, *_):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            registry = FakeRegistry([])
            app = self.prepare_fake(root, registry)
            (root / "demo-1.0.0.crate").write_bytes(b"changed")
            with self.assertRaisesRegex(publisher.PublishError, "checksum"):
                app.publish({"demo": package("demo")}, ["demo"])
            self.assertFalse(registry.calls)

    @patch.object(publisher, "fingerprint", return_value={"revision": "fixed"})
    def test_ready_prepare_does_not_invoke_cargo(self, *_):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            app = self.prepare_fake(root, FakeRegistry([]))
            with patch.object(publisher.subprocess, "run", side_effect=AssertionError("Cargo must not run")):
                app.prepare({"demo": package("demo")}, ["demo"], {}, no_verify=True)

    @patch.object(publisher, "fingerprint", return_value={"revision": "fixed"})
    def test_unverified_receipt_is_not_treated_as_verified(self, *_):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            app = self.prepare_fake(root, FakeRegistry([]))
            metadata = {"packages": [{"id": "demo", "name": "demo"}], "workspace_members": ["demo"]}
            with patch.object(publisher.subprocess, "run", side_effect=publisher.PublishError("verification needed")) as cargo:
                with self.assertRaisesRegex(publisher.PublishError, "verification needed"):
                    app.prepare({"demo": package("demo")}, ["demo"], metadata, no_verify=False)
                self.assertNotIn("--no-verify", cargo.call_args.args[0])

    @patch.object(publisher, "fingerprint", return_value={"revision": "fixed"})
    def test_changed_metadata_blocks_upload(self, *_):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            registry = FakeRegistry([])
            app = self.prepare_fake(root, registry)
            app.entry("demo", "1.0.0")["metadata"]["vers"] = "2.0.0"
            with self.assertRaisesRegex(publisher.PublishError, "metadata checksum"):
                app.publish({"demo": package("demo")}, ["demo"])
            self.assertFalse(registry.calls)


if __name__ == "__main__":
    unittest.main()
