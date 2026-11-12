"""核对原始配置及数量，不执行仓库变换。"""
from .io import asset,unique
def summarize(v,root):
    unique(v['chunks'],'id')
    for c in v['chunks']:asset(root,c['scan'])
    for r in v['stream']:
        if r['kind'] not in ('transaction','marker'):raise ValueError('来源操作未知')
    return {'chunks':len(v['chunks']),'stream_records':len(v['stream']),'initial_rows':len(v['initial_mirror'])}
