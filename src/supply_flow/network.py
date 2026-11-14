"""Deterministic Edmonds-Karp residual flow with per-edge reverse arcs."""
from collections import deque

def _augment(adjacency, arcs, source, sink):
    total=0
    while True:
        parent={source:None};queue=deque([source])
        while queue and sink not in parent:
            u=queue.popleft()
            for i in adjacency[u]:
                _,v,residual=arcs[i]
                if residual>0 and v not in parent:parent[v]=i;queue.append(v)
        if sink not in parent:break
        path=[];v=sink
        while v!=source:
            i=parent[v];path.append(i);v=arcs[i][0]
        amount=min(arcs[i][2] for i in path)
        for i in path:arcs[i][2]-=amount;arcs[i^1][2]+=amount
        total+=amount
    reachable={source};queue=deque([source])
    while queue:
        for i in adjacency[queue.popleft()]:
            _,v,residual=arcs[i]
            if residual>0 and v not in reachable:reachable.add(v);queue.append(v)
    return total,reachable

def maximum_flow(nodes,edges,source,sink):
    adjacency={n:[] for n in nodes};arcs=[];original=[]
    for u,v,cap in edges:
        i=len(arcs);arcs.extend([[u,v,cap],[v,u,0]]);adjacency[u].append(i);adjacency[v].append(i+1);original.append((i,cap))
    total,reachable=_augment(adjacency,arcs,source,sink)
    return total,[cap-arcs[i][2] for i,cap in original],reachable

def bounded_flow(nodes,edges,source,sink,lower=None,upper=None,required=None):
    """Feasible integer s-t flow with per-edge lower/upper capacity bounds.

    Uses the standard super-source/super-sink circulation reduction and the same
    deterministic Edmonds-Karp augmentation as maximum_flow: no third-party
    solver and no per-unit capacity enumeration. When required is None a feasible
    flow is augmented to maximum throughput; otherwise source-to-sink throughput
    must equal required exactly. Returns (feasible, flows) with flows aligned to
    edges when feasible, otherwise (False, None).
    """
    m=len(edges)
    if lower is None:lower=[0]*m
    if upper is None:upper=[edge[2] for edge in edges]
    if len(lower)!=m or len(upper)!=m or any(lo<0 or lo>hi for lo,hi in zip(lower,upper)):
        return False,None
    index={name:k for k,name in enumerate(nodes)}
    super_source,super_sink=len(nodes),len(nodes)+1
    adjacency={k:[] for k in range(len(nodes)+2)};arcs=[];balance=[0]*len(nodes);original=[]
    def add(u,v,cap):
        i=len(arcs);arcs.extend([[u,v,cap],[v,u,0]]);adjacency[u].append(i);adjacency[v].append(i+1);return i
    def install(u,v,lo,hi):
        i=add(u,v,hi-lo);original.append((i,lo,hi));balance[u]-=lo;balance[v]+=lo
    for k,(u,v,_cap) in enumerate(edges):
        install(index[u],index[v],lower[k],upper[k])
    s,t=index[source],index[sink];ceiling=sum(upper)+1
    if required is None:
        install(t,s,0,ceiling)
    else:
        if required<0:return False,None
        install(t,s,required,required)
    need=0
    for v,amount in enumerate(balance):
        if amount>0:add(super_source,v,amount);need+=amount
        elif amount<0:add(v,super_sink,-amount)
    moved,_=_augment(adjacency,arcs,super_source,super_sink)
    if moved!=need:return False,None
    if required is None:_augment(adjacency,arcs,s,t)
    flows=[lo+(hi-lo)-arcs[i][2] for i,lo,hi in original[:m]]
    return True,flows
