"""Verify the public Bash entrypoints with a recording Cargo, never a build."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]


class RustScriptsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="claviron-check-scripts-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        cargo = self.directory / "cargo"
        shutil.copyfile(Path(__file__).parent / "fixtures/cargo.sh", cargo)
        cargo.chmod(0o755)
        self.log = self.directory / "commands"
        self.env = dict(os.environ)
        self.env.pop("SQLX_OFFLINE", None)
        self.env.pop("FAIL_COMMAND", None)
        self.env.update(
            PATH=str(self.directory) + os.pathsep + os.defpath,
            CHECK_LOG=str(self.log),
            RUSTFLAGS="--cfg script_fixture",
            CARGO_TARGET_DIR=str(self.directory / "target with spaces"),
        )

    def run_script(self, script, *args, **environment):
        return subprocess.run(
            ["/bin/bash", str(ROOT / "scripts" / script), *args],
            cwd=self.directory, env=dict(self.env, **environment),
            capture_output=True, text=True,
        )

    def calls(self, offline="true"):
        records = []
        for line in self.log.read_bytes().splitlines():
            fields = line.decode().split("\0")[:-1]
            self.assertEqual(fields[:5], [
                str(ROOT), offline, self.env["PATH"],
                self.env["RUSTFLAGS"], self.env["CARGO_TARGET_DIR"],
            ])
            records.append(fields[5:])
        return records

    def test_format_check_and_package_scope(self):
        result = self.run_script("format.sh", "--check", "-p", "claviron-http")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls(), [
            ["fmt", "--package", "claviron-http", "--", "--check"],
            ["clippy", "--workspace", "--all-features", "--all-targets", "--", "-D", "warnings"],
        ])

    def test_explicit_fix_precedes_formatting_and_preserves_live_sqlx(self):
        result = self.run_script("format.sh", "--fix", SQLX_OFFLINE="false")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls(offline="false"), [
            ["fix", "--workspace", "--all-features", "--all-targets", "--allow-dirty", "--allow-staged"],
            ["fmt", "--all"],
            ["clippy", "--workspace", "--all-features", "--all-targets", "--", "-D", "warnings"],
        ])

    def test_test_filter_and_harness_arguments_remain_separate(self):
        result = self.run_script("test.sh", "a filter", "--", "--nocapture", "--exact")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls(), [[
            "test", "--workspace", "--all-features", "--no-fail-fast",
            "a filter", "--", "--nocapture", "--exact",
        ]])

    def test_default_test_run_and_failure_propagation(self):
        result = self.run_script("test.sh", FAIL_COMMAND="test")
        self.assertEqual(result.returncode, 7)
        self.assertNotIn("Tests passed", result.stdout)
        self.assertEqual(self.calls(), [[
            "test", "--workspace", "--all-features", "--no-fail-fast",
        ]])

    def test_failed_formatting_never_starts_clippy(self):
        result = self.run_script("format.sh", FAIL_COMMAND="fmt")
        self.assertEqual(result.returncode, 7)
        self.assertEqual(self.calls(), [["fmt", "--all"]])

    def test_help_and_invalid_options_do_not_invoke_cargo(self):
        for script in ("format.sh", "test.sh"):
            self.assertEqual(self.run_script(script, "--help").returncode, 0)
        for script, args in (
            ("format.sh", ["--fix", "--check"]),
            ("format.sh", ["-p"]),
            ("format.sh", ["--unknown"]),
            ("test.sh", ["--package", "claviron-http"]),
            ("test.sh", ["one", "two"]),
        ):
            self.assertEqual(self.run_script(script, *args).returncode, 2)
        self.assertFalse(self.log.exists())
