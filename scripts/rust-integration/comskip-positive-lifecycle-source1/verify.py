"""Small public-only Source projection/corruption checks; no app or decoder."""
import importlib.util,json,shutil,sys,tempfile
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('positive_business',ROOT/'business.py');business=importlib.util.module_from_spec(spec);spec.loader.exec_module(business)
def project(root,label):
    source=ROOT/'cells'/label;row=business.load(source,'SQL-after');row=row['recordings'][0]if label.startswith('natural-')else row
    return business.project(root,label,source_cell=source,recording_file=row['custom_properties']['file_path'],effective_ini=row['custom_properties']['comskip']['ini_path'],ini_role='default-packaged-ini'if label in('positive-cut','cut-repeat')else'custom-positive-ini',recording_id_map={row['id']:row['id']},is_source=True)['business']
def main():
    expected=json.loads((ROOT/'business-projection.json').read_text())['cells']
    for label in business.LABELS:business.equal(project(ROOT/'cells'/label,label),expected[label])
    cases=[('SQL-Comskip-commercials','positive-mark','SQL-after',lambda v:v['custom_properties']['comskip'].__setitem__('commercials',99)),('Recording-owned-PK','positive-cut','SQL-after',lambda v:v.__setitem__('id',999)),('Automatic-task-parent','natural-automatic-cut','journal',lambda v:v['publications'][2]['raw_frame']['headers'].__setitem__('parent_id',v['publications'][0]['id'])),('Automatic-task-UUID-reuse','natural-automatic-cut','journal',lambda v:v['publications'][2].__setitem__('id',v['publications'][0]['id'])),('Backend-causal-clock','positive-mark','backend',lambda v:v.__setitem__('date_done','2099-01-01T00:00:00+00:00')),('Receiver-group-add-binding','positive-mark','receiver-ready',lambda v:v.__setitem__('receiver','specific.positive_00000000000040008000000000000000!receiver'))]
    checked=[]
    with tempfile.TemporaryDirectory(prefix='positive-business-public-controls-')as tmp:
        for name,label,filename,mutation in cases:
            target=Path(tmp)/name;shutil.copytree(ROOT/'cells'/label,target);path=target/(filename+'.json');value=json.loads(path.read_text());mutation(value);path.write_text(json.dumps(value))
            try:business.equal(project(target,label),expected[label])
            except(AssertionError,ValueError,KeyError):checked.append(name);continue
            raise AssertionError('Corruption accepted:'+name)
    print(json.dumps({'passed':len(checked)==6,'actual_Source_projections':6,'corruptions_rejected':checked,'new_requests_roles_decoders':0,'Native_acceptance':False}))
if __name__=='__main__':main()
