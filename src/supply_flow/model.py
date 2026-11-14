from .common import require,ident,number,rows
from .network import maximum_flow

def analyze(data):
    require(data,'nodes links supplies demands');nodes=[]
    for n in rows(data['nodes'],100):
        ident(n)
        if n.startswith('__'):raise ValueError('reserved internal node prefix')
        if n in nodes:raise ValueError('duplicate node')
        nodes.append(n)
    if not nodes:raise ValueError('network needs nodes')
    edges=[];descriptions=[];link_ids=set()
    for link in sorted(rows(data['links'],500),key=lambda x:str(x.get('id')) if isinstance(x,dict) else ''):
        require(link,'id from to capacity');lid=ident(link['id']);u=ident(link['from']);v=ident(link['to']);cap=number(link['capacity'])
        if lid in link_ids or u not in nodes or v not in nodes or u==v:raise ValueError('invalid link identity or endpoint')
        link_ids.add(lid);edges.append((u,v,cap));descriptions.append({'kind':'link','id':lid,'from':u,'to':v,'capacity':cap})
    sources={};demands={}
    for key,values in [('supplies',sources),('demands',demands)]:
        for x in rows(data[key],100):
            require(x,'node amount');n=ident(x['node']);amount=number(x['amount'])
            if n not in nodes or n in values:raise ValueError('duplicate/unknown supply or demand node')
            values[n]=amount
    if set(sources)&set(demands):raise ValueError('supply and demand nodes must be disjoint')
    for n,cap in sorted(sources.items()):
        edges.append(('__source',n,cap));descriptions.append({'kind':'supply','id':n,'from':'__source','to':n,'capacity':cap})
    for n,cap in sorted(demands.items()):
        edges.append((n,'__sink',cap));descriptions.append({'kind':'demand','id':n,'from':n,'to':'__sink','capacity':cap})
    total,flows,reachable=maximum_flow(sorted(nodes)+['__source','__sink'],edges,'__source','__sink')
    cut=[dict(d,flow=f) for d,f in zip(descriptions,flows) if d['from'] in reachable and d['to'] not in reachable]
    supplied={};served={};balance={n:0 for n in nodes};link_flows=[]
    for d,f in zip(descriptions,flows):
        if not 0<=f<=d['capacity']:raise ValueError('internal capacity violation')
        if d['from'] in balance:balance[d['from']]-=f
        if d['to'] in balance:balance[d['to']]+=f
        if d['kind']=='supply':supplied[d['id']]=f
        elif d['kind']=='demand':served[d['id']]=f
        else:link_flows.append(dict(d,flow=f))
    if any(balance.values()) or sum(d['capacity'] for d in cut)!=total:raise ValueError('internal flow/cut certificate mismatch')
    return {'total_served':total,'total_demand':sum(demands.values()),'total_available_supply':sum(sources.values()),
            'supply_used':supplied,'demand_served':served,'unmet_demand':{n:demands[n]-served[n] for n in sorted(demands)},
            'links':link_flows,'minimum_cut_capacity':sum(d['capacity'] for d in cut),'cut_edges':cut,
            'residual_reachable_nodes':sorted(n for n in nodes if n in reachable)}
