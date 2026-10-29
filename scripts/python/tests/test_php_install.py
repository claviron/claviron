"""Exercise installer policy with recording commands; never build or install PHP."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
INSTALL = ROOT / "scripts/install/php"
FIXTURES = Path(__file__).parent / "fixtures"


class PhpInstallTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="php-install-policy-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.source = self.directory / "source"
        self.source.mkdir()
        self.bin = self.directory / "bin"
        self.bin.mkdir()
        self.fixture = FIXTURES / "php-build-command.sh"
        for name in ["cc", "c++", "uname", "make", "gmake", "pecl"]:
            self.command(self.bin / name)
        for name in ["configure", "buildconf", "config.status"]:
            self.command(self.source / name)
        (self.source / "Makefile").touch()
        (self.source / "main").mkdir()
        for name in ["php_config.h", "build-defs.h"]:
            (self.source / "main" / name).touch()
        self.log = self.directory / "commands"
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith(("PHP", "FIXTURE", "CFLAGS", "CXXFLAGS", "CPPFLAGS", "LDFLAGS", "PKG_CONFIG"))}
        self.env.update(
            HOME=str(self.directory / "home"),
            PATH=str(self.bin) + os.pathsep + os.defpath,
            INSTALLER=str(INSTALL / "php-zts-install.sh"),
            MODULE_INSTALLER=str(INSTALL / "php-zts-install-modules.sh"),
            COMMAND_FIXTURE=str(self.fixture),
            CHECK_LOG=str(self.log),
            PHP_SRC_DIR=str(self.source),
            PHP_INSTALL_DIR=str(self.directory / "prefix"),
            PHP_VERSION="8.5.12", MAKE_JOBS="1", CC="cc", CXX="c++",
            CPPFLAGS="", LDFLAGS="", PKG_CONFIG_PATH="",
        )

    def command(self, path):
        shutil.copyfile(self.fixture, path)
        path.chmod(0o755)

    def run_harness(self, action, **environment):
        return subprocess.run(
            ["/bin/bash", str(FIXTURES / "php-build-harness.sh"), action],
            cwd=self.directory, env=dict(self.env, **environment),
            capture_output=True, text=True,
        )

    def calls(self, name):
        return [line.decode().split("\0")[:-1]
                for line in self.log.read_bytes().splitlines()
                if line.startswith(name.encode() + b"\0")]

    def test_release_overrides_inherited_unoptimized_flags(self):
        result = self.run_harness("flags", CFLAGS="-I/example/include -O0 -g3")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("-I/example/include -O0 -g3 -O2 -g0", result.stdout)
        self.assertIn("--disable-debug", result.stdout)

    def test_debug_defaults_are_separate(self):
        self.env.pop("PHP_SRC_DIR")
        self.env.pop("PHP_INSTALL_DIR")
        result = self.run_harness("flags", PHP_BUILD_PROFILE="debug")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("/opt/php-zts-embed-debug", result.stdout)
        self.assertIn("/src/php-src-debug", result.stdout)
        self.assertIn("-O0 -g", result.stdout)
        result = self.run_harness("flags", PHP_BUILD_PROFILE="debug", PHP_INSTALL_DIR="/opt/php-zts-embed")
        self.assertNotEqual(result.returncode, 0)

    def test_existing_release_prefix_rejects_debug_replacement(self):
        prefix = self.directory / "prefix/bin"
        prefix.mkdir(parents=True)
        self.command(prefix / "php")
        result = self.run_harness("profile", PHP_BUILD_PROFILE="debug")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("separate prefix", result.stderr)

    def test_old_objects_are_cleaned_and_unchanged_retry_reuses_them(self):
        old = self.source / "old.o"
        old.touch()
        result = self.run_harness("build")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(old.exists())
        self.assertIn("--disable-debug", self.calls("configure")[0])
        self.assertEqual(self.calls("configure")[0][1], "-O2 -g0")
        self.assertEqual(len(self.calls("configure")), 1)
        old.touch()
        result = self.run_harness("build", FAIL_MAKE="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(old.exists())
        self.assertEqual(len(self.calls("configure")), 1)
        result = self.run_harness("build")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(old.exists())
        self.assertEqual(len(self.calls("configure")), 1)
        result = self.run_harness("build", COMPILER_REVISION="2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(old.exists())
        self.assertEqual(len(self.calls("configure")), 2)

    def test_failed_reconfiguration_cannot_reuse_old_stamp(self):
        self.assertEqual(self.run_harness("build").returncode, 0)
        result = self.run_harness("build", FIXTURE_RECONFIGURE="1", FAIL_CONFIGURE="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.source / ".claviron-php-build").exists())
        self.assertFalse((self.source / "Makefile").exists())
        result = self.run_harness("build")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.calls("configure")), 3)

    def test_missing_makefile_still_discards_old_objects(self):
        (self.source / "Makefile").unlink()
        old = self.source / "old.o"
        old.touch()
        result = self.run_harness("build")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(old.exists())

    def test_freebsd_clean_uses_gmake(self):
        result = self.run_harness("clean", FIXTURE_OS="FreeBSD")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([record[-1] for record in self.calls("gmake")], ["clean", "distclean"])
        self.assertEqual(self.calls("make"), [])

    def test_debug_does_not_register_system_library(self):
        result = self.run_harness("register", PHP_BUILD_PROFILE="debug")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("unexpected loader operation", result.stderr)

    def test_pecl_gets_profile_flags_and_cleans_only_its_private_directory(self):
        temporary = self.directory / "build-temp"
        temporary.mkdir()
        sentinel = temporary / "other-build"
        sentinel.touch()
        record = self.directory / "pecl-temp"
        result = self.run_harness(
            "module", CFLAGS="-I/example/include -O0", TMPDIR=str(temporary),
            FIXTURE_PECL=str(self.bin / "pecl"), PECL_TEMP_RECORD=str(record),
            FIXTURE_MARKER=str(self.directory / "loaded"),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls("pecl")[0][1], "-I/example/include -O0 -O2 -g0")
        private = Path(record.read_text().strip())
        self.assertEqual(private.parent, temporary)
        self.assertFalse(private.exists())
        self.assertTrue(sentinel.exists())

    def test_service_installer_help_does_not_install(self):
        for name in ["php-zts-fpm-install.sh", "php-zts-fpm-install-freebsd.sh"]:
            with self.subTest(name=name):
                result = subprocess.run(["/bin/bash", str(INSTALL / name)], env=self.env,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("Usage:", result.stdout)


if __name__ == "__main__":
    unittest.main()
