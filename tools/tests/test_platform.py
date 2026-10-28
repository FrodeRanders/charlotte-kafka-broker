"""Regression checks for exact-run deployment readiness and platform pins."""
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class PlatformTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "broker with spaces"
        (self.root / "tools").mkdir(parents=True)
        (self.root / ".charlotte").mkdir()
        self.platform = self.root / "tools/platform.sh"
        shutil.copyfile(Path(__file__).resolve().parents[1] / "platform.sh", self.platform)
        (self.root / "charlotte.lock").write_text("OS_REVISION=current\nTOOLCHAIN=nightly-test\n")
        self.environment = self.root / ".charlotte/platform.env"
        self.environment.write_text(
            "CHARLOTTE_BUILD_ELF=/builder\nCHARLOTTE_OS_REVISION=current\n"
            "CHARLOTTE_TOOLCHAIN=nightly-test\n"
        )
        self.os_dir = self.root / "platform"
        self.runs = self.os_dir / "target/deployment-ingress-test"
        self.log = self.root / "runner.log"

    def ready(self):
        return subprocess.run(
            ["bash", "-c", '. "$1"; broker_deployment_ready "$2" "$3"',
             "test", str(self.platform), str(self.os_dir), str(self.log)],
            capture_output=True,
        ).returncode == 0

    def test_prior_readiness_does_not_satisfy_current_run(self):
        old = self.runs / "run.old"
        current = self.runs / "run.current"
        old.mkdir(parents=True)
        current.mkdir()
        (old / "result").write_text("ready\n")
        self.log.write_text("")
        self.assertFalse(self.ready())
        self.log.write_text(f'signed deployment {current}/broker.cdep: artifact="broker"\n')
        self.assertFalse(self.ready())
        (current / "result").write_text("ready\n")
        self.assertTrue(self.ready())

    def test_unrelated_directory_cannot_supply_readiness(self):
        other = self.root / "other"
        other.mkdir()
        (other / "result").write_text("ready\n")
        self.log.write_text(f'signed deployment {other}/broker.cdep: artifact="broker"\n')
        self.assertFalse(self.ready())

    def test_stale_platform_resolution_is_rejected(self):
        self.environment.write_text(
            "CHARLOTTE_BUILD_ELF=/builder\nCHARLOTTE_OS_REVISION=old\n"
            "CHARLOTTE_TOOLCHAIN=nightly-test\n"
        )
        result = subprocess.run(
            ["bash", "-c", '. "$1"', "test", str(self.platform)], capture_output=True,
        )
        self.assertNotEqual(0, result.returncode)
        self.assertIn(b"stale", result.stderr)


if __name__ == "__main__":
    unittest.main()
