"""Lazy regex dependency entry: stdlib/regex only; no application imports."""
import argparse,os,sys
from pathlib import Path

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--mode',choices=('regex',),required=True);parser.add_argument('--source');parser.add_argument('--plugins');parser.parse_args()
 reader=os.fdopen(os.dup(0),'rb',buffering=0);writer=os.fdopen(os.dup(1),'wb',buffering=0)
 null=os.open(os.devnull,os.O_RDONLY);os.dup2(null,0);os.close(null);os.dup2(2,1)
 sys.stdin=open(os.devnull);sys.stdout=sys.stderr
 sys.path.insert(0,str(Path(__file__).resolve().parent))
 from dispatcharr_native.wire import Channel,install
 from dispatcharr_native.regex_only import serve
 channel=Channel(reader,writer);install(channel);serve(channel)
if __name__=='__main__':main()
