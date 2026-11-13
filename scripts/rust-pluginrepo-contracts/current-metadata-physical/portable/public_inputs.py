"""Strict selected public references only; no private/original fallback."""
from pathlib import Path
import gzip,hashlib,json
ROOT=Path(__file__).resolve().parents[1]
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def need(ok,label):
 if not ok:raise ValueError(label)
def load_source():
 policy=json.loads((ROOT/'source-reference.json').read_text());p=ROOT/policy['path'];need(digest(p)==policy['selected_compressed_sha256'],'SelectedSourceCompressedSHA');raw=gzip.decompress(p.read_bytes());need(len(raw)==policy['selected_JSON_bytes']and hashlib.sha256(raw).hexdigest()==policy['selected_JSON_sha256'],'SelectedSourceJSONSHA');return json.loads(raw)
def load_contracts():
 manifest=json.loads((ROOT/'packet-files.json').read_text());p=ROOT/'data/pg-contracts.json';need(digest(p)==manifest['data/pg-contracts.json'],'SelectedExactContractsSHA');return json.loads(p.read_text())
