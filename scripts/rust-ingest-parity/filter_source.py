"""Capture actual original builtin-re filter behavior; preview regex is separate.

Original Dispatcharr authors retain application/algorithm credit. This captures
reference behavior, not acceptance of a replacement regex engine.
"""
import argparse
import ast
import hashlib
import json
import logging
from pathlib import Path
import random
import re
import sys
import types
import unicodedata
ROOT=Path(__file__).resolve().parents[2]

def original():
    logger=logging.getLogger('source-filter');logger.disabled=True;logger.trace=lambda *args,**kwargs:None
    scope={'logger':logger,'re':re}
    tree=ast.parse((ROOT/'apps/m3u/tasks.py').read_text());names={'_compile_m3u_stream_filters','_stream_passes_m3u_filters'}
    nodes=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(ROOT/'apps/m3u/tasks.py'),'exec'),scope)
    return scope

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    scope=original();rng=random.Random(20261109)
    patterns=['','News','^News$','foo$','foo\\Z',r'\w+',r'\d+',r'\s+',r'\b',r'\B',r'\b\w+\b',
        '[a-z]+','[A-Z]+','[I]','i','s','k',r'(?a)\w+',r'(?a:\w+)',r'(?u)\w+',r'(?i)news',r'(?i:news)',
        r'(?m)^foo$',r'(?s)a.b',r'(?x) a \# b #comment',r'(foo)\1',r'(?P<label>foo)(?P=label)',
        r'(?<=ab)c',r'(?<!ab)c',r'foo(?=bar)',r'foo(?!bar)',r'(a)?(?(1)b|c)',r'(?>a|ab)c',r'a*+a',
        r'[\d\w\s]',r'[^\w]',r'[[]',r'[a&&b]',r'\123',r'\12',r'\0',r'\08',r'\x41',r'\u0130',
        r'\U000105C0',r'\N{LATIN CAPITAL LETTER A}',r'\a',r'\v',r'\b[\b]',r'\Afoo',r'a{2,4}?',
        r'a{,3}',r'a{2,}',r'a{bad}',r'(a|b)+',r'(?-i:News)',r'(?i:a(?-i:b))',r'a(?i)b',r'(?q)a',
        r'(?L)a',r'(?au)a',r'(?P<x>a)(?P<x>b)',r'(?<=a+)b',r'(?<=a|bc)d',r'(',r'[',r'a**',r'\z',
        r'\p{Letter}',r'\k<x>',r'(?R)',r'(?|a|b)',r'(?=a)*',r'(a?)\1',r'(?:)',r'(?!)',r'\w{0}']
    texts=['','News','NEWS','news\n','foo\n','foo\n\n','foobar','foofoo','abc','ababc','ac','c',
        'aab','aaab','ab','abbb','a\nb','a#b','012','١٢','１２','²','αβ','Café','İIıiſSKK','\x1c','\x1f','\u0085','\u00a0','\u200b','\U000105c0','\0','\b','a'*20,'!','😀']
    cases=[]
    for ignore in [False,True]:
        for pattern in patterns:
            try:
                compiled=re.compile(pattern,re.IGNORECASE if ignore else 0)
                expected={'compiled':True,'matches':[compiled.search(text) is not None for text in texts]}
            except Exception as error:
                expected={'compiled':False,'error_type':type(error).__name__,'message':str(error),'position':getattr(error,'pos',None)}
            cases.append({'pattern':pattern,'ignore_case':ignore,'expected':expected})
    policy=[]
    for index in range(500):
        filters=[]
        for order in range(rng.randrange(8)):
            filters.append({'regex_pattern':rng.choice(['News','Sport','foo','^http','Group','.',r'\b\w+\b','(?i)abc']),
                            'filter_type':rng.choice(['name','url','group','legacy-type']), 'exclude':rng.choice([False,True]),
                            'custom_properties':rng.choice([{},None,{'case_sensitive':False},{'case_sensitive':0}]),'order':order})
        compiled=scope['_compile_m3u_stream_filters']([types.SimpleNamespace(**item) for item in filters])
        targets={'name':rng.choice(['News','Sport','abc','foo','',None]),'url':rng.choice(['http://fixture','udp://fixture','',None]),'group':rng.choice(['Group','News','',None])}
        expected=scope['_stream_passes_m3u_filters'](targets['name'],targets['url'],targets['group'],compiled)
        policy.append({'filters':filters,'targets':targets,'expected':expected})
    report={'scope':'original-builtin-Python-re-M3U-filters','python':sys.version,'unicode_version':unicodedata.unidata_version,
        'source_sha256':hashlib.sha256((ROOT/'apps/m3u/tasks.py').read_bytes()).hexdigest(),
        'tool_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'texts':texts,'regex_cases':cases,'policy_cases':policy,
        'native_filter_parity_verified':False,'notes':['Built-in re task filters are distinct from third-party regex preview/transform behavior.','Patterns and input strings are deliberately bounded; this source capture exercises no catastrophic large backtracking input.']}
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'regex_cases':len(cases),'policy_cases':len(policy),'python':sys.version.split()[0],'unicode':unicodedata.unidata_version}))

if __name__=='__main__':main()
