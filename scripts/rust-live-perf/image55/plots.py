#!/usr/bin/env python3
"""Standalone scientific figure from verified complete live numeric projection.
Original Dispatcharr authors retain application/algorithm credit and licensing.
"""
import argparse,hashlib,importlib.util,json
from pathlib import Path
spec=importlib.util.spec_from_file_location('_live_figure_check',Path(__file__).with_name('verify_projection.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
def render(source,output):
 m.verify(source);data=json.loads(Path(source).read_text());output=Path(output);output.mkdir(parents=True,exist_ok=False)
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 import statistics
 pairs=data['report']['pairs'];fig,axes=plt.subplots(1,4,figsize=(18,4.8));names=[('CPU','Active live backend + PG/Redis/origin CPU',.9),('memory','Peak working memory',.95),('throughput','Measured delivered bytes',.95),('first_byte_p95','First-byte p95 (50 ms floor)',1.1)]
 for axis,(key,title,budget)in zip(axes,names):
  for x,n in enumerate([1,10,100],1):
   ys=[p['ratios'][key]for p in pairs if p['viewers']==n];axis.scatter([x-.07,x,x+.07],ys,s=28);axis.plot([x-.2,x+.2],[statistics.median(ys)]*2,c='black',lw=2)
  axis.axhline(budget,c='firebrick',ls='--',label='fixed gate');axis.set_xticks([1,2,3],['1','10','100']);axis.set_xlabel('Viewers');axis.set_ylabel('Native / Original');axis.set_title(title,fontsize=10);axis.set_ylim(bottom=0);axis.legend(fontsize=8)
 fig.suptitle('Image 55 authenticated live TS — three alternating pairs per cohort; scoped service group')
 fig.text(.5,.01,'15 s warmup / 60 s measurement; continuous authenticated readers; source-bound images; no full-application claim',ha='center',fontsize=9);fig.tight_layout(rect=(0,.04,1,.93))
 for extension in ['png','svg']:fig.savefig(output/('live-paired-ratios.'+extension),dpi=180)
 svg=output/'live-paired-ratios.svg';svg.write_text('\n'.join(line.rstrip()for line in svg.read_text().splitlines())+'\n')
 plt.close(fig);(output/'figure-source.json').write_text(json.dumps({'projection_sha256':hashlib.sha256(Path(source).read_bytes()).hexdigest(),'credit':data['credit'],'scope':data['scope']},indent=2)+'\n')
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--projection',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();render(a.projection,a.output)
