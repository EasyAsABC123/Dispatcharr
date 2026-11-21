"""Read only the package database of the exact native public donor."""
from hashlib import file_digest
import os
from pathlib import Path
import re
import subprocess
import sys


if __name__ == '__main__':
    assert os.uname().sysname == 'Linux' and os.uname().machine == 'aarch64'
    assert os.getuid() == 65532
    arch = subprocess.check_output(['/usr/bin/dpkg','--print-architecture'],timeout=5)
    assert arch == b'arm64\n'
    with Path('/usr/local/bin/comskip').open('rb') as file:
        assert file_digest(file,'sha256').hexdigest() == '75bc4daf372684c366fbd86a3d53106f12d1be571a63ed4d55bc28d400ab4692'
    release=Path('/etc/os-release').read_bytes()
    assert re.search(rb'^VERSION_CODENAME="?resolute"?$', release, re.MULTILINE)
    status=Path('/var/lib/dpkg/status').read_bytes()
    assert 0<len(status)<4*1024*1024 and status.startswith(b'Package: ')
    sys.stdout.buffer.write(status)
