"""The demo consumes one verified SDK, including paths containing spaces."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest

class SdkTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'demo with spaces'
        (self.root/'tools').mkdir(parents=True)
        self.tool = self.root/'tools/charlotte-sdk.sh'
        shutil.copyfile(Path(__file__).resolve().parents[1]/'charlotte-sdk.sh',self.tool)
        self.tool.chmod(0o755)
        (self.root/'charlotte.lock').write_text('OS_REVISION=fixture\nSITAS_REVISION=sitas-fixture\nTOOLCHAIN=nightly-fixture\nSDK_SCHEMA=2\n')
        self.source = Path(self.temp.name)/'source/charlotte-app-sdk'
        for name in ['charlotte-app','catten-rt','charlotte-launch','charlotte-kafka','sitas-core','sitas-charlotte','sitas-unix']:
            folder = self.source/'crates'/name
            folder.mkdir(parents=True)
            (folder/'Cargo.toml').write_text(f'[package]\nname="{name}"\n')
        self.manifest = self.source/'sdk.json'
        self.write_manifest()
        self.archive = Path(self.temp.name)/'sdk.tar.gz'
        self.pack()

    def write_manifest(self, revision='fixture'):
        files = {str(p.relative_to(self.source)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in self.source.rglob('*') if p.is_file() and p != self.manifest}
        self.manifest.write_text(json.dumps(dict(os_revision=revision,sitas_revision='sitas-fixture',
            toolchain='nightly-fixture',schema=2,files=files)))

    def pack(self):
        with tarfile.open(self.archive,'w:gz') as archive:
            archive.add(self.source,arcname='charlotte-app-sdk')

    def invoke(self,*args,expected=0):
        env=os.environ.copy(); env.pop('CHARLOTTE_SDK_SHA256',None)
        result=subprocess.run([str(self.tool),*map(str,args)],cwd=self.root,text=True,
                              capture_output=True,env=env)
        self.assertEqual(result.returncode==0,expected==0,result.stdout+result.stderr)
        return result

    def test_hashes_and_shell_paths_are_checked_before_use(self):
        self.invoke('unpack',self.archive)
        self.assertIn('demo with spaces',self.invoke('status').stdout)
        installed=self.root/'.charlotte/sdk/charlotte-app-sdk/crates/catten-rt/Cargo.toml'
        installed.write_text('substituted')
        self.assertIn('SDK input differs',self.invoke('verify',expected=1).stderr)

    def test_wrong_revision_does_not_replace_the_selected_sdk(self):
        self.invoke('unpack',self.archive)
        selected=self.root/'.charlotte/sdk/charlotte-app-sdk/sdk.json'
        original=selected.read_bytes()
        self.write_manifest(revision='other'); self.pack()
        self.invoke('unpack',self.archive,expected=1)
        self.assertEqual(selected.read_bytes(),original)

    def test_archive_sidecar_is_enforced(self):
        Path(str(self.archive)+'.sha256').write_text('0'*64+'  sdk.tar.gz\n')
        self.assertIn('sha256 mismatch',self.invoke('unpack',self.archive,expected=1).stderr)
