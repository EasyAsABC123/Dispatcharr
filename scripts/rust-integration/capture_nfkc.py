"""Capture original Python Unicode assignment boundaries and NFKC vectors.

Only public Unicode data is emitted; application credit remains with original
Dispatcharr contributors. Run in the pinned original runtime.
"""
import json
import unicodedata as u
ranges=[];start=None;cases=[];lower=[]
for code in range(0x110000):
    char=chr(code);assigned=u.category(char)!='Cn'
    if assigned and start is None:start=code
    if not assigned and start is not None:ranges.append([start,code-1]);start=None
    normalized=u.normalize('NFKC',char)
    if normalized!=char:cases.append([char,normalized])
    lowered=char.lower()
    if lowered!=char:lower.append([char,lowered])
if start is not None:ranges.append([start,0x10ffff])
samples=['A\u0301','\uff21\u0301','rust-probe-'+chr(0x1ccd6)+'\u0301','A'+chr(0x1ccd6)+'\u0301',chr(0xa7f1),chr(0x1ccf0),'\u00a0A\u3000','\ufb03\u212a\u2163','\u1100\u1161\u11a8']
print(json.dumps({'unicode_version':u.unidata_version,'assigned_ranges':ranges,'changed_codepoints':cases,'lower_changed_codepoints':lower,'lower_strings':[[v,v.lower()]for v in ['ΟΣ.COM','User@EXAMPLE.INVALID','İΣΣ '+chr(0xa7dc)+'Σ','A'+chr(0x1ccd6)+'Σ']],'strings':[[v,u.normalize('NFKC',v)]for v in samples]},ensure_ascii=True,separators=(',',':')))
