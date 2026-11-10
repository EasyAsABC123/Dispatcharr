"""Draft original strict-fragment goldens; execute only after quiet ends.
All application/algorithm credit belongs to original Dispatcharr authors.
Functions are compiled unchanged from the pinned mounted original source.
Owned local sentinels/loopback resources only; no foreign file/network access.
"""
import argparse
import ast
import base64
import hashlib
import html.entities
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
from pathlib import Path
import random
import tempfile
import threading
from lxml import etree

def tree(node):
    return {'tag':node.tag,'text':node.text,'attrs':dict(node.attrib),'children':[tree(child) for child in node if isinstance(child.tag,str)]}

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    original=Path('/source/apps/epg/tasks.py').read_bytes();names={'_build_html_entity_doctype','_parse_programme_element'}
    nodes=[node for node in ast.parse(original).body if isinstance(node,ast.FunctionDef) and node.name in names]
    if {node.name for node in nodes}!=names:raise RuntimeError('Pinned original helper definitions missing')
    scope={'html':html,'etree':etree,'_XML_ENTITIES':{'amp','lt','gt','quot','apos'}}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<original strict programme helper>','exec'),scope);scope['_HTML_ENTITY_DOCTYPE']=scope['_build_html_entity_doctype']()
    bodies=[
      ('empty',b''),('root',b'<programme/>'),('other-root',b'<other/>'),('space',b' \n<programme/>\n '),
      ('text',b'<programme><title> A &eacute; &amp; B </title><desc> x\n y </desc></programme>'),
      ('blank-text',b'<programme> \n <title>   </title><desc>\t\r\n</desc></programme>'),
      ('nested',b'<programme><programme><title>Inner</title></programme><title>Outer</title></programme>'),
      ('direct-text',b'<programme><title>before<b>bold</b>after</title></programme>'),
      ('comment-text',b'<programme><title>before<!--comment-->after</title></programme>'),
      ('pi-text',b'<programme><title>before<?fixture value?>after</title></programme>'),
      ('cdata',b'<programme><title><![CDATA[a]]><![CDATA[b]]>c</title></programme>'),
      ('namespace',b'<programme xmlns="urn:fixture"><title>a</title></programme>'),
      ('namespaced-attrs',b'<programme xmlns:x="urn:fixture" x:channel="one"><x:title>x</x:title><title x:lang="en">y</title></programme>'),
      ('duplicate-attribute',b'<programme channel="first" channel="last"/>'),
      ('unknown-entity',b'<programme><title>&unknown;</title></programme>'),
      ('html5-only-entity',b'<programme><title>&NotEqualTilde;</title></programme>'),
      ('missing-semicolon',b'<programme><title>&copy</title></programme>'),
      ('bare-ampersand',b'<programme><title>a & b</title></programme>'),
      ('truncated',b'<programme><title>unfinished'),('mismatched',b'<programme><title>x</programme>'),
      ('trailing-root',b'<programme/><programme/>'),('leading-text',b'text<programme/>'),('trailing-text',b'<programme/>text'),
      ('utf8-bom',b'\xef\xbb\xbf<programme/>'),('xml-declaration',b'<?xml version="1.0"?><programme/>'),
      ('invalid-utf8',b'<programme><title>\xff</title></programme>'),('invalid-nul',b'<programme><title>\x00</title></programme>'),
      ('numeric-nul',b'<programme><title>&#0;</title></programme>'),('numeric-surrogate',b'<programme><title>&#xD800;</title></programme>'),
      ('html4-all',b'<programme><title>'+b''.join(('&'+name+';').encode() for name in sorted(html.entities.name2codepoint))+b'</title></programme>'),
    ]
    rng=random.Random(20261129)
    for index in range(256):
        body=b'<programme>'
        for _ in range(rng.randrange(1,7)):
            body+=rng.choice([b'<title>x</title>',b'<desc>  </desc>',b'<sub-title>&eacute;</sub-title>',b'<title>before<b>x</b>after</title>',b'<!--x-->',b'<?fixture x?>',b'<programme/>',b'<title>',b'</desc>',b'\x00',b'&unknown;'])
        body+=b'</programme>';bodies.append(('random-'+str(index),body))
    requests=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            requests.append(self.path);payload=b'<!ENTITY owned "owned-loopback-entity">';self.send_response(200);self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
    controls={};cases=[]
    try:
        with tempfile.TemporaryDirectory(prefix='dispatcharr-strict-owned-') as directory:
            sentinel=Path(directory)/'entity.txt';marker='owned-local-entity-fixture';sentinel.write_text(marker);uri=sentinel.as_uri();network='http://127.0.0.1:'+str(server.server_address[1])+'/owned.dtd'
            bodies.extend([('owned-file-reference',('<!DOCTYPE programme [<!ENTITY ext SYSTEM "'+uri+'">]><programme>&ext;</programme>').encode()),('owned-network-reference',('<!DOCTYPE programme SYSTEM "'+network+'"><programme>&owned;</programme>').encode())])
            # This control deliberately omits the helper's injected prefix. It
            # records the underlying Source parser's owned local-file behavior;
            # it is not a positive claim about the strict helper's DTD handling.
            direct=etree.fromstring(('<!DOCTYPE programme [<!ENTITY ext SYSTEM "'+uri+'">]><programme>&ext;</programme>').encode(),etree.XMLParser(resolve_entities=True,load_dtd=True,no_network=True))
            controls['owned_local_parser_reads_marker']=direct.text==marker
            before=len(requests)
            for name,body in bodies:
                try:value=scope['_parse_programme_element'](body);result={'kind':'ok','tree':tree(value)}
                except etree.XMLSyntaxError:result={'kind':'syntax'}
                cases.append({'name':name,'input_base64':base64.b64encode(body).decode(),'expected':result})
            controls['helper_network_requests']=len(requests)-before
            controls['helper_no_network_observed']=len(requests)==before
            controls['owned_external_scope']='Only these helper inputs and uniquely owned sentinel/loopback resources; not general external-loader parity.'
    finally:
        server.shutdown();server.server_close();worker.join(timeout=2)
    result={'format_version':1,'scope':'Original strict single-element helper; source capture only','cases':cases,'controls':controls,'source_sha256':hashlib.sha256(original).hexdigest(),'tool_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'html_entity_doctype_sha256':hashlib.sha256(scope['_HTML_ENTITY_DOCTYPE']).hexdigest(),'python':os.sys.version,'lxml_version':etree.LXML_VERSION,'libxml_version':etree.LIBXML_VERSION,'native_verified':False,'app_wired':False,'performance_verified':False}
    args.output.write_text(json.dumps(result,ensure_ascii=True,indent=2)+'\n')

if __name__=='__main__':main()
