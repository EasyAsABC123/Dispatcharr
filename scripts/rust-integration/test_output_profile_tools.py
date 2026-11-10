"""Probe-helper namespace checks; no Django, database, Docker or credentials.
Original Dispatcharr authors retain application credit.
"""
import ast
from pathlib import Path
import unittest
ROOT=Path(__file__).resolve().parent
class ProbeHelpers(unittest.TestCase):
    def test_frozen_source_wire_runs_in_declared_packaged_namespace(self):
        probe=ast.parse((ROOT/'output_profile_packaged_probe.py').read_text())
        allowed={'base64','json','urllib.parse'}
        imports=[node for node in probe.body if
            isinstance(node,ast.Import)and all(alias.name in allowed for alias in node.names)
            or isinstance(node,ast.ImportFrom)and node.module in allowed]
        source=ast.parse((ROOT/'output_profile_fixture.py').read_text())
        wire=next(node for node in source.body if isinstance(node,ast.FunctionDef)and node.name=='wire')
        namespace={}
        exec(compile(ast.Module(body=imports+[wire],type_ignores=[]),'trusted-wire-only','exec'),namespace)
        self.assertEqual(namespace['wire']({'raw':'ew=='}),(b'{','application/json'))
        self.assertEqual(namespace['wire']({'form':{'name':'hello world'}}),
                         (b'name=hello+world','application/x-www-form-urlencoded'))
        self.assertEqual(namespace['wire']({'json':{'name':'é'}}),
                         (b'{"name":"\xc3\xa9"}','application/json'))
        data,mime=namespace['wire']({'multipart':{'name':'value'}})
        self.assertEqual(mime,'multipart/form-data; boundary=rust-probe-output-boundary')
        self.assertTrue(data.endswith(b'--rust-probe-output-boundary--\r\n'))
    def test_witness_namespace_constants_are_declared(self):
        tree=ast.parse((ROOT/'output_profile_packaged_probe.py').read_text())
        constants={node.targets[0].id:ast.literal_eval(node.value)for node in tree.body
                   if isinstance(node,ast.Assign)and len(node.targets)==1
                   and isinstance(node.targets[0],ast.Name)and node.targets[0].id in ['PREFIX','KEYS']}
        self.assertEqual(constants['PREFIX'],'rust-probe-output-profile-')
        self.assertEqual(len(constants['KEYS']),6)
if __name__=='__main__':unittest.main()
