DESCRIPTION='Integer maximum-flow allocation with verifiable minimum cut'
import argparse, json, sys
from pathlib import Path
from .common import read, save
from .model import analyze, ALLOCATION_MODES

def run(source, allocation='max-flow'):
    data,digest=read(source)
    return {'schema_version':1,'source_sha256':digest,'result':analyze(data,allocation=allocation)}

def main():
    parser=argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--allocation',choices=ALLOCATION_MODES,default='max-flow',
                        help='max-flow (default): original total-only logic; fair: max total, then max-min served/amount, then lexicographic service vector')
    args=parser.parse_args()
    try:
        if args.source.resolve()==args.output.resolve():raise ValueError('input and output must differ')
        report=run(args.source,args.allocation);save(args.output,report)
    except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError) as error:
        print(str(error),file=sys.stderr);return 2
    print(json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

if __name__=='__main__':raise SystemExit(main())
