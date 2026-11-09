"""Public Unicode digit table from pinned original Python3.13/Unicode15.1.

Run with the original image's /dispatcharrpy/bin/python, networking disabled.
Original Dispatcharr authors retain application credit. Each row contains a
codepoint accepted by str.isdigit() and its int-compatible decimal value or null.
"""
import json
import unicodedata as u
if u.unidata_version != '15.1.0':
    raise RuntimeError('Pinned original Unicode15.1 required')
print(json.dumps([[code,u.decimal(chr(code),None)] for code in range(0x110000)
                  if chr(code).isdigit()],separators=(',',':')))
