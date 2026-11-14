import unittest,json,itertools
from pathlib import Path
from supply_flow.model import analyze
from supply_flow.network import maximum_flow
ROOT=Path(__file__).resolve().parents[1]
class FlowTests(unittest.TestCase):
    def setUp(self):self.d=json.loads((ROOT/'fixtures/input.json').read_text())
    def test_known_bottleneck_and_shortage(self):
        r=analyze(self.d);self.assertEqual(r['total_served'],7);self.assertEqual(r['demand_served'],{'east':4,'west':3})
        self.assertEqual(r['minimum_cut_capacity'],7);self.assertEqual(r['unmet_demand']['west'],2)
    def test_disconnected_network_and_zero_supply(self):
        self.d['links']=[];self.assertEqual(analyze(self.d)['total_served'],0)
        self.d['supplies']=[];self.assertEqual(analyze(self.d)['total_available_supply'],0)
    def test_parallel_antiparallel_edges_and_cycle(self):
        nodes=['S','A','B','T'];edges=[('S','A',3),('S','A',2),('A','S',8),('A','B',4),('B','A',3),('B','T',4)]
        total,flows,reachable=maximum_flow(nodes,edges,'S','T');self.assertEqual(total,4)
        self.assertTrue(all(0<=f<=e[2] for e,f in zip(edges,flows)))
    def test_residual_reverse_arcs_reallocate_earlier_choice(self):
        nodes=['S','A','B','C','D','T'];edges=[('S','A',1),('S','B',1),('A','C',1),('A','D',1),('B','C',1),('C','T',1),('D','T',1)]
        self.assertEqual(maximum_flow(nodes,edges,'S','T')[0],2)
    def test_bruteforce_cuts_independently_certify_small_graphs(self):
        nodes=['S','A','B','T']
        for caps in itertools.product(range(3),repeat=4):
            edges=list(zip(['S','S','A','B'],['A','B','T','T'],caps))+[('A','B',1)]
            cuts=[]
            for bits in itertools.product((0,1),repeat=2):
                subset={'S'}|{n for n,b in zip(['A','B'],bits) if b}
                cuts.append(sum(c for u,v,c in edges if u in subset and v not in subset))
            self.assertEqual(maximum_flow(nodes,edges,'S','T')[0],min(cuts))
    def test_order_invariance(self):
        r=analyze(self.d);self.d['nodes'].reverse();self.d['links'].reverse();self.assertEqual(r,analyze(self.d))
    def test_invalid_capacity_unknown_reference_and_overlap(self):
        self.d['links'][0]['capacity']=-1
        with self.assertRaises(ValueError):analyze(self.d)
        self.setUp();self.d['demands'][0]['node']='reservoir'
        with self.assertRaises(ValueError):analyze(self.d)
if __name__=='__main__':unittest.main()
