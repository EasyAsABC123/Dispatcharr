"""Strict dependency on published projected Source24; never private-hash fallback."""
import gzip,hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
need=lambda ok,label:None if ok else (_ for _ in()).throw(ValueError(label))
H=lambda raw:hashlib.sha256(raw).hexdigest()
def load_source():
 need(H((ROOT/'manifest.json').read_bytes())=='7d184f7204881acbfec1acaecb4e01d2e5a76b8be9db8d7a84b9bddcf2a6932b','ExactManifestSourceBinding');m=json.loads((ROOT/'manifest.json').read_bytes())['public_Source_dependency'];base=ROOT/m['relative_prefix'];need(H((base/'projection-policy.json').read_bytes())==m['policy_sha256'],'ExactPublishedProjectionPolicy');compressed=(base/m['selected_reference_file']).read_bytes();need(H(compressed)==m['selected_compressed_sha256'],'StrictSelectedSourceReferenceOnly');raw=gzip.decompress(compressed);need(len(raw)<=67108864 and H(raw)==m['selected_JSON_sha256'],'StrictSelectedSourceJSONNoOriginalFallback');return json.loads(raw)
