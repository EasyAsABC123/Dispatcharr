"""Run offline native build binding and image retention tests."""
from hashlib import sha256
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest

import local_build
import local_image

JOB="728490182734827383-1"
TAG=f"dispatcharr-rust-arm64-local:{JOB}"
ID="sha256:"+"a"*64
INTERMEDIATE="sha256:"+"b"*64
FOREIGN="sha256:"+"c"*64


class Bundle(unittest.TestCase):
    def setUp(self):
        self.directory=TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.folder=Path(self.directory.name)
        names=("orchestrate.py","assembly-manifest.json","Dockerfile.app.arm64","local_image.py")
        self.ledger="".join(f"{sha256(name.encode()).hexdigest()}  {name}\n" for name in names).encode()
        for name in names:
            (self.folder/name).write_text(name)

    def test_bundle_admits_sealed_files(self):
        self.assertEqual(len(local_build.bundle(self.folder,self.ledger)),4)

    def test_bundle_declines_unsealed_edit(self):
        (self.folder/"orchestrate.py").write_text("different source")
        with self.assertRaises(AssertionError):
            local_build.bundle(self.folder,self.ledger)

    def test_duplicate_entry_rejected(self):
        with self.assertRaises(AssertionError):
            local_build.bundle(self.folder,self.ledger+self.ledger.splitlines(keepends=True)[0])

    def test_exact_native_dispatch(self):
        command=local_build.command(self.folder,self.folder/"evidence",JOB)
        self.assertIn(local_build.APP,command)
        self.assertEqual(command[-4:],["--evidence",str(self.folder/"evidence"),"--local-image-tag",TAG])
        self.assertNotIn("--push",command)


class Retention(unittest.TestCase):
    def invoke(self, path, actual=ID, arch="arm64", retirement_failure=False):
        self.calls=[]
        def docker(*args):
            self.calls.append(args)
        def image(tag):
            self.calls.append(("inspect",tag))
            return {"Id":actual,"Os":"linux","Architecture":arch,"Config":{"User":"65532:65532"}}
        def retire(owner, prior, acquired):
            self.calls.append(("retire",owner,tuple(sorted(prior)),tuple(acquired)))
            if retirement_failure:
                raise subprocess.CalledProcessError(1,"docker")
        local_image.retain_local_image("owner",{FOREIGN},[INTERMEDIATE,ID],ID,TAG,path,docker,image,retire)

    def test_cleanup_precedes_local_tag_and_exact_receipt(self):
        with TemporaryDirectory() as work:
            path=Path(work)
            self.invoke(path)
            self.assertEqual(self.calls,[("retire","owner",(FOREIGN,),(INTERMEDIATE,)),
                                         ("image","tag",ID,TAG),("inspect",TAG)])
            receipt=json.loads((path/"local-build.json").read_bytes())
            self.assertEqual(receipt["image_config"],ID)
            self.assertFalse(receipt["compose_executed"])
            self.assertFalse(receipt["image_pushed"])

    def test_failed_intermediate_cleanup_does_not_tag(self):
        with TemporaryDirectory() as work:
            with self.assertRaises(subprocess.CalledProcessError):
                self.invoke(Path(work),retirement_failure=True)
            self.assertEqual([row[0] for row in self.calls],["retire"])

    def test_reject_wrong_arch_and_remove_only_owned_tag(self):
        with TemporaryDirectory() as work:
            with self.assertRaises(AssertionError):
                self.invoke(Path(work),arch="amd64")
            self.assertEqual(self.calls[-1],("image","rm",TAG))

    def test_foreign_tag_identity_is_never_removed(self):
        with TemporaryDirectory() as work:
            with self.assertRaises(AssertionError):
                self.invoke(Path(work),actual=FOREIGN)
            self.assertNotIn(("image","rm",TAG),self.calls)


if __name__=="__main__":
    unittest.main()
