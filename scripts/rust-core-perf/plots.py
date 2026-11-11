#!/usr/bin/env python3
"""Standalone plots from independently validated numeric projection, no workloads."""
import argparse,json,hashlib,os
from pathlib import Path
os.environ.setdefault('MPLCONFIGDIR','/tmp/dispatcharr-m1-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def render(source,output):
 data=json.loads(Path(source).read_text())
 if data.get('performance_verified') is not True or len(data['pairs'])!=9:raise ValueError('Validated complete projection required')
 output=Path(output);output.mkdir(parents=True,exist_ok=True)
 fig,axes=plt.subplots(1,3,figsize=(12,3.8))
 for ax,key,title,budget in [(axes[0],'cpu_ratio','Whole core + PG/Redis CPU',.9),(axes[1],'memory_ratio','Peak working memory',.95)]:
  for pos,v in enumerate([1,10,100]):
   ys=[r[key]for r in data['pairs']if r['viewers']==v];ax.scatter([pos-.08,pos,pos+.08],ys,s=28,label='paired trials'if pos==0 else None);ax.plot([pos-.2,pos+.2],[data['cohort_medians'][str(v)][key]]*2,color='black',lw=2)
  ax.axhline(budget,color='firebrick',ls='--',label='fixed budget');ax.set(title=title,ylabel='Native / Original',xticks=[0,1,2],xticklabels=['1','10','100'],xlabel='Readers',ylim=(0,1.06));ax.legend(fontsize=8)
 ax=axes[2]
 for pos,v in enumerate([1,10,100]):
  for i,r in enumerate(x for x in data['pairs']if x['viewers']==v):
   for kind,y in r['measured_class_byte_ratios'].items():ax.scatter(pos+(i-1)*.1,y,marker='o'if kind=='fast'else'x',color='tab:blue'if kind=='fast'else'tab:orange')
 ax.scatter([],[],marker='o',color='tab:blue',label='fast readers');ax.scatter([],[],marker='x',color='tab:orange',label='slow readers');ax.legend(fontsize=8,loc='lower left')
 ax.axhline(.98,color='firebrick',ls='--');ax.set(title='Measured per-class byte work',ylabel='min(Original, Native) / max',xticks=[0,1,2],xticklabels=['1','10','100'],xlabel='Readers',ylim=(.96,1.01))
 fig.suptitle('Actual shared-buffer core benchmark — three alternating pairs per reader cohort',fontsize=11)
 fig.text(.5,.005,'15s warmup / 60s measurement; fixed Source-only pacing; zero lifecycle OOM; exact final bytes; owned cleanup verified',ha='center',fontsize=8)
 fig.tight_layout(rect=(0,.05,1,.94))
 for ext in ['svg','png']:fig.savefig(output/('core-paired-ratios.'+ext),dpi=180,metadata={'Description':'Projection SHA256 '+hashlib.sha256(Path(source).read_bytes()).hexdigest()}if ext=='svg'else None)
 svg=output/'core-paired-ratios.svg'
 svg.write_text(''.join(line.rstrip()+'\n' for line in svg.read_text().splitlines()))
 plt.close(fig)
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--projection',required=True);p.add_argument('--output',required=True);a=p.parse_args();render(a.projection,a.output)
