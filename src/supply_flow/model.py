from fractions import Fraction
from .common import require,ident,number,rows
from .network import maximum_flow,bounded_flow

ALLOCATION_MODES=('max-flow','fair')

def analyze(data, allocation='max-flow'):
    if allocation not in ALLOCATION_MODES:raise ValueError('unknown allocation mode: '+str(allocation))
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
    flow_nodes=sorted(nodes)+['__source','__sink']
    # Level 1 objective for every mode: maximum total served, plus a minimum-cut
    # certificate computed by the original residual algorithm.
    total,maxflows,reachable=maximum_flow(flow_nodes,edges,'__source','__sink')
    if allocation=='fair':
        fairflows,ratio=_fair_flows(flow_nodes,edges,descriptions,demands,total)
        flows=fairflows
    else:
        flows=maxflows;ratio=None
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
    if sum(served.values())!=total:raise ValueError('internal total flow mismatch')
    result={'total_served':total,'total_demand':sum(demands.values()),'total_available_supply':sum(sources.values()),
            'supply_used':supplied,'demand_served':served,'unmet_demand':{n:demands[n]-served[n] for n in sorted(demands)},
            'links':link_flows,'minimum_cut_capacity':sum(d['capacity'] for d in cut),'cut_edges':cut,
            'residual_reachable_nodes':sorted(n for n in nodes if n in reachable)}
    if allocation=='fair':
        result['allocation_mode']='fair';result['min_service_ratio']={'numerator':ratio[0],'denominator':ratio[1]}
    return result

def _fair_flows(flow_nodes,edges,descriptions,demands,total):
    """Lexicographic fair allocation among demand nodes with positive demand.

    First the total stays at the maximum total. Then the minimum exact ratio
    served/amount over positive-demand nodes is maximized; zero-demand nodes do
    not participate. Ties are broken by lexicographically maximizing the service
    vector over demand-node names ascending. All decisions use exact integer
    arithmetic and bounded_flow (super-source/sink circulation reduction).
    """
    m=len(edges)
    demand_arc={d['id']:k for k,d in enumerate(descriptions) if d['kind']=='demand'}
    positive=sorted(n for n,amount in demands.items() if amount>0)
    upper=[d['capacity'] for d in descriptions]
    lower=[0]*m
    if not positive:
        # No positive demand: the ratio is defined as 1 and only level 3 applies.
        ratio=(1,1)
    else:
        ratio=_max_min_ratio(flow_nodes,edges,lower,upper,demand_arc,demands,positive,total)
        if ratio[0]:
            p,q=ratio
            for n in positive:
                a=demands[n]
                lower[demand_arc[n]]=(p*a + q-1)//q  # ceil(p*a/q), exact integer ceil
        # ratio (0,1) leaves every lower bound at zero
    # Level 3 tie-break: maximize the service vector lexicographically by node name.
    for n in sorted(demands):  # zero-demand nodes sort alongside but stay fixed at zero
        arc=demand_arc[n]
        if demands[n]==0:
            lower[arc]=0;continue
        probe=lower[:]
        # First test whether one more unit on this arc is even feasible. When it
        # is, augmenting with that lower bound pushes the arc first and usually
        # lands directly on its maximum; one follow-up feasibility test proves
        # maximality. Only if the proof fails do we fall back to binary search.
        probe[arc]=lower[arc]+1
        feasible,priority=bounded_flow(flow_nodes,edges,'__source','__sink',lower=probe,upper=upper,required=total)
        if not feasible:
            value=lower[arc]
        else:
            proof=lower[:];proof[arc]=priority[arc]+1
            maximal,_=bounded_flow(flow_nodes,edges,'__source','__sink',lower=proof,upper=upper,required=total)
            value=priority[arc] if not maximal else _best_value(flow_nodes,edges,lower,upper,arc,total)
        lower[arc]=value
    feasible,flows=bounded_flow(flow_nodes,edges,'__source','__sink',lower=lower,upper=upper,required=total)
    if not feasible:raise ValueError('internal fair allocation infeasibility')
    return flows,ratio

def _best_value(flow_nodes,edges,lower,upper,arc,total):
    """Largest feasible flow on one demand arc under the fixed prefix (binary search)."""
    lo=lower[arc];hi=upper[arc]
    while lo<hi:
        mid=(lo+hi+1)//2;candidate=lower[:];candidate[arc]=mid
        feasible,_=bounded_flow(flow_nodes,edges,'__source','__sink',lower=candidate,upper=upper,required=total)
        if feasible:lo=mid
        else:hi=mid-1
    return lo

def _max_min_ratio(flow_nodes,edges,lower,upper,demand_arc,demands,positive,total):
    """Exact max-min of served/amount via scaled integer feasibility tests."""
    M=max(demands[n] for n in positive)
    def scaled_feasible(p,q):
        candidate=lower[:]
        for n in positive:
            a=demands[n];candidate[demand_arc[n]]=(p*a+q-1)//q
        feasible,_=bounded_flow(flow_nodes,edges,'__source','__sink',lower=candidate,upper=upper,required=total)
        return feasible
    if not scaled_feasible(1,M):return (0,1)  # minimum ratio is exactly zero
    # Phase A: locate the optimum within width 1/M using integer binary search.
    lo,hi=0,M
    while lo<hi:
        mid=(lo+hi+1)//2
        if scaled_feasible(mid,M):lo=mid
        else:hi=mid-1
    if lo==M:return (1,1)  # every positive demand can be fully served
    low,high=Fraction(lo,M),Fraction(lo+1,M)
    # Phase B: the optimum equals f/amount for some tight demand node. Every
    # attainable value inside the bracket is one of at most two ints per node.
    candidates=set()
    for n in positive:
        a=demands[n]
        first=-(-low.numerator*a//low.denominator)      # ceil(low*a), exact
        last=high.numerator*a//high.denominator         # floor(high*a)
        for f in range(first,last+1):
            if 0<=f<=a:
                frac=Fraction(f,a)
                if low<=frac<=high:candidates.add(frac)
    ordered=sorted(candidates,reverse=True)
    # Feasibility is monotone: a feasible suffix. Binary search for the first feasible.
    left,right=0,len(ordered)
    while left<right:
        mid=(left+right)//2
        frac=ordered[mid]
        if scaled_feasible(frac.numerator,frac.denominator):right=mid
        else:left=mid+1
    best=ordered[left]
    return (best.numerator,best.denominator)
