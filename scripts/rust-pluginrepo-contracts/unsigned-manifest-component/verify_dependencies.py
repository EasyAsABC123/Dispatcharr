"""Installed RECORD/MD5/license authority, read-only bytes; no imports of deps."""
import base64,csv,hashlib,io,json
from pathlib import Path
import verify as V
ROOT=Path(__file__).resolve().parent
need=V.need;H=V.H
def raw_input(name):
 mapping=json.loads((ROOT/'closure-materialization.json').read_bytes())['epochs']['Source24']['entries'];row=mapping[name];raw=(ROOT/row['exact_public_blob']).read_bytes();need(H(raw)==row['original_sha256']and len(raw)==row['original_bytes'],'ExactDependencyBlob');return raw
def validate():
 manifest=json.loads((ROOT/'licenses-manifest.json').read_bytes());entries=json.loads((ROOT/'closure-materialization.json').read_bytes())['epochs']['Source24']['entries'];count=0;license_count=0
 for package,version in [('requests','2.34.2'),('urllib3','2.8.0')]:
  prefix='inputs/dependency-provenance/distinfo4/actual-image/site-packages/'+package+'-'+version+'.dist-info/';metadata=raw_input(prefix+'METADATA');need(('Version: '+version+'\n').encode()in metadata,'ActualInstalledVersion');record={row[0]:(row[1],row[2])for row in csv.reader(io.StringIO(raw_input(prefix+'RECORD').decode()))}
  selected=[name for name in entries if name.startswith('inputs/actual-image/site-packages/'+package+'/')and name.endswith('.py')]
  for name in selected:
   rel=name.removeprefix('inputs/actual-image/site-packages/');raw=raw_input(name);digest,size=record[rel];expected='sha256='+base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).decode().rstrip('=');need(digest==expected and str(len(raw))==size,'AllActualPackageSourceRECORDBytes');count+=1
  for row in manifest['requests_urllib3_actual_installed_license_files']:
   rel=row['file'].split('site-packages/',1)[-1]
   if not rel.startswith(package+'-'):continue
   raw=(ROOT/row['file']).read_bytes();need(H(raw)==row['sha256']and len(raw)==row['bytes'],'ExactInstalledLicense');digest,size=record[rel];need(digest=='sha256='+base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).decode().rstrip('=')and size==str(len(raw)),'LicenseActualRECORDBytes');license_count+=1
 need(count==39 and license_count==3,'Exact39PackageSources3LicenseFiles');psf=manifest['PSF_stdlib_four_files'];licensebytes=(ROOT/psf['actual_installed_copyright']).read_bytes();need(H(licensebytes)==psf['sha256']and hashlib.md5(licensebytes).hexdigest()==psf['installed_md5'],'ActualInstalledPythonCopyright');stdlib=0
 for row in psf['actual_four_source_bindings']:
  name='inputs/actual-image/stdlib/'+row['runtime_path'].removeprefix('/usr/lib/python3.13/');raw=raw_input(name);need(H(raw)==row['source_sha256']and hashlib.md5(raw).hexdigest()==row['installed_md5'],'ActualStdlibInstalledDpkgBindings');stdlib+=1
 need(stdlib==4,'ExactFourStdlibSources');return {'passed':True,'Requests_version':'2.34.2','urllib3_version':'2.8.0','package_source_RECORD_matches':count,'package_license_RECORD_matches':license_count,'stdlib_dpkg_matches':stdlib,'PSF_installed_copyright_exact':True,'metadata_partial_HOST1_preserved':True,'Source_program_imports_apps':0}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
