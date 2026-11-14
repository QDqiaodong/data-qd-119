"""Deterministic Edmonds-Karp residual flow with per-edge reverse arcs."""
from collections import deque

def maximum_flow(nodes,edges,source,sink):
    adjacency={n:[] for n in nodes};arcs=[];original=[]
    for u,v,cap in edges:
        i=len(arcs);arcs.extend([[u,v,cap],[v,u,0]]);adjacency[u].append(i);adjacency[v].append(i+1);original.append((i,cap))
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
    return total,[cap-arcs[i][2] for i,cap in original],reachable
