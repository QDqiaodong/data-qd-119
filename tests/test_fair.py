import unittest, itertools, random
from fractions import Fraction
from supply_flow.model import analyze
from supply_flow.network import bounded_flow, maximum_flow


def _augmented_edges(data):
    edges=[]
    for link in sorted(data['links'], key=lambda x:x['id']):
        edges.append((link['from'],link['to'],link['capacity']))
    src={x['node']:x['amount'] for x in data['supplies']}
    for n in sorted(src):edges.append(('__source',n,src[n]))
    arc_of={}
    for d in data['demands']:
        arc_of[d['node']]=len(edges)
        edges.append((d['node'],'__sink',d['amount']))
    nodes=sorted(data['nodes'])+['__source','__sink']
    return nodes,edges,arc_of


def brute_force_fair(data):
    """Independently enumerate every integer demand vector and rank the
    feasible ones by the same three-level objective (total, min exact ratio,
    lexicographic service vector over names ascending)."""
    total=analyze(data)['total_served']
    demands={d['node']:d['amount'] for d in data['demands']}
    names=sorted(demands)
    nodes,edges,arc_of=_augmented_edges(data)
    best=None
    for vec in itertools.product(*[range(demands[n]+1) for n in names]):
        if sum(vec)!=total:continue
        lower=[0]*len(edges)
        for n,v in zip(names,vec):lower[arc_of[n]]=v
        feasible,_=bounded_flow(nodes,edges,'__source','__sink',lower=lower,required=total)
        if not feasible:continue
        ratios=[Fraction(v,demands[n]) for n,v in zip(names,vec) if demands[n]>0]
        ratio=min(ratios) if ratios else Fraction(1,1)
        key=(-ratio,tuple(-v for v in vec))
        if best is None or key<best[0]:
            best=(key,ratio,dict(zip(names,vec)))
    if best is None:  # total == 0: the zero vector is the unique feasible choice
        return total,Fraction(1,1),{n:0 for n in names}
    return total,best[1],best[2]


def random_network(rng):
    internal=['n%d'%i for i in range(rng.randint(2,5))]
    pairs=list(itertools.permutations(internal,2))
    rng.shuffle(pairs)
    links=[{'id':'L%02d'%i,'from':u,'to':v,'capacity':rng.randint(0,4)}
           for i,(u,v) in enumerate(pairs[:rng.randint(1,min(7,len(pairs)))])]
    sup=rng.choice(internal)
    supplies=[{'node':sup,'amount':rng.randint(0,5)}]
    candidates=[n for n in internal if n!=sup]
    rng.shuffle(candidates)
    demands=[{'node':n,'amount':rng.choice([0,0,1,2,3,4])}
             for n in candidates[:rng.randint(1,len(candidates))]]
    return {'nodes':internal,'links':links,'supplies':supplies,'demands':demands}


class FairBruteForceTests(unittest.TestCase):
    def test_three_level_objective_matches_exhaustive_enumeration(self):
        rng=random.Random(31337)
        checked=0
        for _ in range(250):
            data=random_network(rng)
            try:
                report=analyze(data, allocation='fair')
                total,ratio,vector=brute_force_fair(data)
            except ValueError:
                continue
            checked+=1
            with self.subTest(data=data):
                served={n:report['demand_served'][n] for n in vector}
                self.assertEqual(served,vector)
                self.assertEqual(report['total_served'],total)
                self.assertEqual(report['min_service_ratio'],
                                 {'numerator':ratio.numerator,'denominator':ratio.denominator})
                self.assertEqual(report['allocation_mode'],'fair')
        self.assertGreater(checked,100)

    def test_fair_keeps_the_maximum_total_of_plain_mode(self):
        rng=random.Random(555)
        for _ in range(100):
            data=random_network(rng)
            try:
                plain=analyze(data);fair=analyze(data, allocation='fair')
            except ValueError:
                continue
            self.assertEqual(plain['total_served'],fair['total_served'])
            self.assertEqual(fair['minimum_cut_capacity'],fair['total_served'])


class FairEdgeCaseTests(unittest.TestCase):
    def assertFair(self,data):
        report=analyze(data, allocation='fair')
        total,ratio,vector=brute_force_fair(data)
        self.assertEqual({n:report['demand_served'][n] for n in vector},vector)
        self.assertEqual(report['min_service_ratio'],
                         {'numerator':ratio.numerator,'denominator':ratio.denominator})
        self.assertEqual(report['total_served'],total)
        self.assertEqual(analyze(data)['total_served'],total)
        return report

    def test_unreachable_demand_has_zero_ratio(self):
        data={'nodes':['s','a','b'],'links':[{'id':'L1','from':'s','to':'a','capacity':5}],
              'supplies':[{'node':'s','amount':5}],
              'demands':[{'node':'a','amount':3},{'node':'b','amount':4}]}
        report=self.assertFair(data)
        self.assertEqual(report['demand_served'],{'a':3,'b':0})
        self.assertEqual(report['min_service_ratio'],{'numerator':0,'denominator':1})

    def test_zero_demand_nodes_do_not_participate(self):
        data={'nodes':['s','a','b'],'links':[{'id':'L1','from':'s','to':'a','capacity':5},
                                              {'id':'L2','from':'s','to':'b','capacity':5}],
              'supplies':[{'node':'s','amount':9}],
              'demands':[{'node':'a','amount':0},{'node':'b','amount':4}]}
        report=self.assertFair(data)
        self.assertEqual(report['demand_served'],{'a':0,'b':4})
        self.assertEqual(report['min_service_ratio'],{'numerator':1,'denominator':1})

    def test_no_positive_demand_defines_ratio_one(self):
        data={'nodes':['s','a'],'links':[{'id':'L1','from':'s','to':'a','capacity':5}],
              'supplies':[{'node':'s','amount':5}],'demands':[{'node':'a','amount':0}]}
        report=analyze(data, allocation='fair')
        self.assertEqual(report['total_served'],0)
        self.assertEqual(report['min_service_ratio'],{'numerator':1,'denominator':1})

    def test_no_supply_serves_nothing_with_ratio_zero(self):
        data={'nodes':['s','a'],'links':[{'id':'L1','from':'s','to':'a','capacity':5}],
              'supplies':[],'demands':[{'node':'a','amount':3}]}
        report=analyze(data, allocation='fair')
        self.assertEqual(report['total_served'],0)
        self.assertEqual(report['demand_served'],{'a':0})
        self.assertEqual(report['min_service_ratio'],{'numerator':0,'denominator':1})
        self.assertEqual(report['cut_edges'],[])

    def test_parallel_and_antiparallel_edges(self):
        data={'nodes':['s','a','b'],'links':[
            {'id':'L1','from':'s','to':'a','capacity':3},{'id':'L2','from':'s','to':'a','capacity':2},
            {'id':'L3','from':'a','to':'s','capacity':8},{'id':'L4','from':'a','to':'b','capacity':4},
            {'id':'L5','from':'b','to':'a','capacity':3}],
            'supplies':[{'node':'s','amount':9}],'demands':[{'node':'b','amount':5}]}
        report=self.assertFair(data)
        self.assertEqual(report['demand_served'],{'b':4})
        self.assertEqual(report['min_service_ratio'],{'numerator':4,'denominator':5})

    def test_disconnected_network(self):
        data={'nodes':['s','a'],'links':[],
              'supplies':[{'node':'s','amount':3}],'demands':[{'node':'a','amount':3}]}
        report=self.assertFair(data)
        self.assertEqual(report['demand_served'],{'a':0})
        self.assertEqual(report['minimum_cut_capacity'],0)

    def test_large_capacity_ratio_stays_exact_and_reduced(self):
        data={'nodes':['s','a','b','c'],'links':[
            {'id':'L1','from':'s','to':'a','capacity':10**9},{'id':'L2','from':'s','to':'b','capacity':10**9},
            {'id':'L3','from':'a','to':'c','capacity':2},{'id':'L4','from':'b','to':'c','capacity':10**9}],
            'supplies':[{'node':'s','amount':10**9}],
            'demands':[{'node':'a','amount':10**9},{'node':'c','amount':6}]}
        report=analyze(data, allocation='fair')
        from math import gcd
        rn=report['min_service_ratio']
        self.assertEqual(gcd(rn['numerator'],rn['denominator']),1)
        self.assertEqual(report['total_served'],10**9)
        self.assertEqual(report['demand_served'],{'a':10**9-6,'c':6})

    def test_fair_changes_allocation_without_losing_total(self):
        # The fixture is served east=4/west=3 by plain mode; fair mode must
        # rebalance to east=3/west=4 while keeping total 7.
        import json
        from pathlib import Path
        data=json.loads((Path(__file__).resolve().parents[1]/'fixtures/input.json').read_text())
        plain=analyze(data);fair=analyze(data, allocation='fair')
        self.assertEqual(plain['demand_served'],{'east':4,'west':3})
        self.assertEqual(fair['demand_served'],{'east':3,'west':4})
        self.assertEqual(plain['total_served'],fair['total_served'],7)
        self.assertEqual(fair['min_service_ratio'],{'numerator':3,'denominator':4})

    def test_lexicographic_tie_break_by_node_name(self):
        # Total 3 over two equal demands of 2: vectors (2,1) and (1,2) share the
        # same optimal minimum ratio 1/2, so the name-ascending vector must win.
        data={'nodes':['s','x','y'],'links':[{'id':'L1','from':'s','to':'x','capacity':2},
                                              {'id':'L2','from':'s','to':'y','capacity':2}],
              'supplies':[{'node':'s','amount':3}],
              'demands':[{'node':'x','amount':2},{'node':'y','amount':2}]}
        report=analyze(data, allocation='fair')
        self.assertEqual(report['demand_served'],{'x':2,'y':1})
        self.assertEqual(report['min_service_ratio'],{'numerator':1,'denominator':2})
        # brute force independently agrees on the tie break
        _,ratio,vector=brute_force_fair(data)
        self.assertEqual(vector,{'x':2,'y':1})
        self.assertEqual((ratio.numerator,ratio.denominator),(1,2))

    def test_deterministic_under_array_permutation(self):
        import copy
        rng=random.Random(12)
        data=random_network(rng)
        first=analyze(data, allocation='fair')
        perm=copy.deepcopy(data)
        rng.shuffle(perm['nodes']);rng.shuffle(perm['links'])
        rng.shuffle(perm['supplies']);rng.shuffle(perm['demands'])
        self.assertEqual(first,analyze(perm, allocation='fair'))
        self.assertEqual(first,analyze(copy.deepcopy(data), allocation='fair'))


class BoundedFlowTests(unittest.TestCase):
    def test_matches_maximum_flow_and_honours_bounds(self):
        rng=random.Random(808)
        for _ in range(100):
            nodes=['S','T']+['v%d'%i for i in range(rng.randint(1,4))]
            edges=[]
            for _ in range(rng.randint(1,10)):
                u,v=rng.sample(nodes,2)
                edges.append((u,v,rng.randint(0,8)))
            total,flows,_=maximum_flow(nodes,edges,'S','T')
            ok,bflows=bounded_flow(nodes,edges,'S','T')
            self.assertTrue(ok)
            self.assertTrue(all(0<=f<=c for (u,v,c),f in zip(edges,bflows)))
            lower=[rng.randint(0,f) for f in flows]
            ok,bflows=bounded_flow(nodes,edges,'S','T',lower=lower)
            self.assertTrue(ok)
            self.assertTrue(all(f>=lo for f,lo in zip(bflows,lower)))
            ok,_=bounded_flow(nodes,edges,'S','T',required=total+1)
            self.assertFalse(ok)
            bad=[c+1 for u,v,c in edges]
            ok,_=bounded_flow(nodes,edges,'S','T',lower=bad)
            self.assertFalse(ok)


if __name__=='__main__':unittest.main()
