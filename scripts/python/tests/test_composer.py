"""Composer export safety; temporary local Git repos only, no builds or network."""

import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import tomllib
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("composer_publisher", Path(__file__).parents[1] / "composer.py")
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)


class ComposerReleaseTests(unittest.TestCase):
    def test_cli_uses_configured_remote_and_allows_explicit_override(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = {
                "state-directory": "target/composer",
                "packages": [{"name": "example/package", "remote": "https://example.test/distribution.git"}],
            }
            document = {"tool": {publisher.PROJECT: {"composer": config}}}
            with patch.object(publisher, "ROOT", root), patch.object(Path, "read_text", return_value=""), \
                 patch.object(tomllib, "loads", return_value=document), \
                 patch.object(publisher, "prepare", return_value=(root / "mirror.git", "prepared")) as prepare, \
                 patch.object(publisher, "publish") as publish, redirect_stdout(io.StringIO()):
                self.assertEqual(publisher.main(["prepare", "--package", "example/package", "--tag", "v1.0.0"]), 0)
                self.assertEqual(prepare.call_args.args[-1], "https://example.test/distribution.git")
                self.assertEqual(publisher.main(["publish", "--package", "example/package", "--tag", "v1.0.0"]), 0)
                self.assertEqual(publish.call_args.args[-2:], ("https://example.test/distribution.git", False))
                self.assertEqual(publisher.main(["publish", "--package", "example/package", "--tag", "v1.0.0", "--remote", "https://example.test/other.git"]), 0)
                self.assertEqual(publish.call_args.args[-2:], ("https://example.test/other.git", False))

    def source(self, root):
        package = {
            "name": "example/package", "directory": "php",
            "license": "LICENSE", "include": ["composer.json", "README.md", "src", "runners"],
        }
        source = root
        publisher.git(source, "init", "--initial-branch=main")
        publisher.git(source, "config", "user.name", "Example Publisher")
        publisher.git(source, "config", "user.email", "publisher@example.test")
        publisher.git(source, "config", "commit.gpgSign", "false")
        publisher.git(source, "config", "core.hooksPath", os.devnull)
        for directory in ("src", "runners", "tests"):
            (source / "php" / directory).mkdir(parents=True, exist_ok=True)
        (source / "LICENSE").write_text("Example license\n")
        (source / "php/README.md").write_text("Example package\n")
        (source / "php/composer.json").write_text(json.dumps({
            "name": "example/package", "version": "1.0.0", "type": "library",
            "autoload": {"psr-4": {"Example\\": "src/"}},
        }))
        (source / "php/src/Example.php").write_text("<?php\nnamespace Example;\n")
        (source / "php/runners/application.php").write_text("<?php\n")
        (source / "php/tests/private.php").write_text("<?php\n")
        publisher.git(source, "add", ".")
        publisher.git(source, "commit", "-m", "Add example package")
        return source, package, {"state-directory": "target/composer"}

    def test_export_is_rooted_minimal_tagged_and_immutable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, package, config = self.source(root)
            # An unrelated dirty file is irrelevant to this committed package.
            (source / "unrelated.txt").write_text("Local work\n")
            mirror, commit = publisher.prepare(root, config, package, "v1.0.0")
            metadata = json.loads(publisher.git(mirror, "show", "v1.0.0:composer.json"))
            self.assertNotIn("version", metadata)
            self.assertEqual(metadata["name"], "example/package")
            self.assertEqual(publisher.git(mirror, "show", "v1.0.0:LICENSE"), b"Example license\n")
            names = publisher.git(mirror, "ls-tree", "-r", "--name-only", commit).decode().splitlines()
            self.assertIn("runners/application.php", names)
            self.assertNotIn("tests/private.php", names)
            self.assertFalse(any(name.startswith("php/") for name in names))
            self.assertEqual(publisher.prepare(root, config, package, "v1.0.0")[1], commit)
            (source / "php/src/Example.php").write_text("<?php\n// changed\n")
            with self.assertRaisesRegex(publisher.ReleaseError, "Commit the selected"):
                publisher.prepare(root, config, package, "v1.0.0")
            publisher.git(source, "add", "php/src/Example.php")
            publisher.git(source, "commit", "-m", "Change example")
            with self.assertRaisesRegex(publisher.ReleaseError, "different bytes"):
                publisher.prepare(root, config, package, "v1.0.0")

    def test_preview_does_not_contact_remote_and_publish_is_repeatable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, package, config = self.source(root)
            mirror, commit = publisher.prepare(root, config, package, "v1.0.0")
            original = publisher.git
            def local_only(directory, *args, **kwargs):
                self.assertNotIn(args[0], ("ls-remote", "fetch", "push"))
                return original(directory, *args, **kwargs)
            with patch.object(publisher, "git", side_effect=local_only), redirect_stdout(io.StringIO()):
                publisher.publish(root, config, package, "v1.0.0", "https://example.test/package.git")
            destination = root / "distribution.git"
            publisher.git(root, "init", "--bare", str(destination))
            with redirect_stdout(io.StringIO()):
                publisher.publish(root, config, package, "v1.0.0", str(destination), send=True)
                publisher.publish(root, config, package, "v1.0.0", str(destination), send=True)
            self.assertEqual(publisher.object_id(destination, "refs/tags/v1.0.0"), commit)
            # Changes after preparation cannot silently change the release bytes.
            (source / "php/src/Example.php").write_text("<?php\n// new work\n")
            self.assertEqual(publisher.git(destination, "show", "v1.0.0:src/Example.php"), b"<?php\nnamespace Example;\n")
            self.assertIsNone(publisher.object_id(source, "refs/tags/v1.0.0"))

    def test_existing_remote_history_is_restored_without_force(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, package, config = self.source(root)
            _, first = publisher.prepare(root, config, package, "v1.0.0")
            destination = root / "distribution.git"
            publisher.git(root, "init", "--bare", str(destination))
            with redirect_stdout(io.StringIO()):
                publisher.publish(root, config, package, "v1.0.0", str(destination), send=True)
            # Simulate a new local release-state directory, not a rewritten remote.
            other = {"state-directory": "other-state"}
            mirror, second = publisher.prepare(root, other, package, "v1.0.1", remote=str(destination))
            self.assertEqual(publisher.object_id(mirror, second + "^"), first)
            with redirect_stdout(io.StringIO()):
                publisher.publish(root, other, package, "v1.0.1", str(destination), send=True)
            self.assertEqual(publisher.object_id(destination, "refs/tags/v1.0.0"), first)
            self.assertEqual(publisher.object_id(destination, "refs/heads/main"), second)

    def test_rejects_nonrelease_tags_and_embedded_credentials(self):
        for tag in ("main", "../bad", "--delete", "v1.0"):
            with self.assertRaises(publisher.ReleaseError):
                publisher.validate_tag(tag)
        with self.assertRaises(publisher.ReleaseError):
            publisher.validate_remote("https://token@example.test/package.git")

    def test_rejects_external_source_repository(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual(publisher.source_repository(root, {}), root.resolve())
            with self.assertRaisesRegex(publisher.ReleaseError, "repository overrides"):
                publisher.snapshot(root, {"repository": "../other"}, "HEAD")

    def test_cli_rejects_unowned_package_before_any_git_operation(self):
        with patch.object(publisher, "git") as git, redirect_stderr(io.StringIO()):
            self.assertEqual(publisher.main(["publish", "--package", "unowned/package", "--tag", "v1.0.0", "--send"]), 1)
            git.assert_not_called()


if __name__ == "__main__":
    unittest.main()
