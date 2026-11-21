"""Shared runtime assembly source and attribution authority."""
from pathlib import Path
import subprocess

ROOT = Path(subprocess.check_output(["git", "-C", str(Path(__file__).resolve().parent),
                                    "rev-parse", "--show-toplevel"], text=True).strip())
CREDIT = "Original Dispatcharr and upstream authors and contributors retain full credit."


def git(*args):
    return subprocess.check_output(["git","-C",str(ROOT),*args],timeout=30)
