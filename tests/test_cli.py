import unittest, tempfile, subprocess, sys, json, hashlib
from pathlib import Path
from supply_flow.model import analyze
ROOT=Path(__file__).resolve().parents[1]
class CLITests(unittest.TestCase):
    def _run(self,source,out,*extra):
        command=[sys.executable,'-m','supply_flow.cli','--source',str(source),'--output',str(out),*extra]
        return subprocess.run(command,capture_output=True)
    def test_installed_cli_and_exact_source_hash(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/'report.json';source=ROOT/'fixtures/input.json'
            result=self._run(source,out)
            self.assertEqual(result.returncode,0,result.stderr)
            report=json.loads(out.read_text());self.assertEqual(report['result'],analyze(json.loads(source.read_text())))
            self.assertNotIn('allocation_mode',report['result'])
            self.assertEqual(report['source_sha256'],hashlib.sha256(source.read_bytes()).hexdigest())
            before=out.read_bytes();self.assertEqual(self._run(source,out).returncode,0)
            self.assertEqual(before,out.read_bytes())
    def test_fair_allocation_flag_matches_python_api_and_changes_vector(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/'fair.json';source=ROOT/'fixtures/input.json'
            result=self._run(source,out,'--allocation','fair')
            self.assertEqual(result.returncode,0,result.stderr)
            report=json.loads(out.read_text())
            expected=analyze(json.loads(source.read_text()),allocation='fair')
            self.assertEqual(report['result'],expected)
            self.assertEqual(report['result']['allocation_mode'],'fair')
            self.assertEqual(report['result']['min_service_ratio'],{'numerator':3,'denominator':4})
            self.assertEqual(report['result']['demand_served'],{'east':3,'west':4})
            # fairness changes the vector but keeps the maximum total and all certificates
            plain=analyze(json.loads(source.read_text()))
            self.assertEqual(plain['total_served'],report['result']['total_served'])
            self.assertEqual(plain['minimum_cut_capacity'],report['result']['minimum_cut_capacity'])
            self.assertNotEqual(plain['demand_served'],report['result']['demand_served'])
            # explicit default mode is equivalent to omitting the flag
            out2=Path(td)/'plain.json'
            self.assertEqual(self._run(source,out2,'--allocation','max-flow').returncode,0)
            self.assertEqual(json.loads(out2.read_text())['result'],plain)
    def test_unknown_allocation_exits_two_without_touching_report(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/'report.json';source=ROOT/'fixtures/input.json';out.write_text('prior')
            result=self._run(source,out,'--allocation','greedy')
            self.assertEqual(result.returncode,2)
            self.assertEqual(out.read_text(),'prior')
    def test_python_api_exposes_allocation_modes(self):
        from supply_flow import analyze as api_analyze, ALLOCATION_MODES
        self.assertEqual(ALLOCATION_MODES,('max-flow','fair'))
        with self.assertRaises(ValueError):
            api_analyze({'nodes':[],'links':[],'supplies':[],'demands':[]},allocation='nope')
    def test_failed_update_retains_prior_output_and_cannot_overwrite_source(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td);source=p/'bad.json';out=p/'report.json';out.write_text('prior')
            for invalid in ['{}','{"a":1,"a":2}','NaN']:
                source.write_text(invalid)
                result=subprocess.run([sys.executable,'-m','supply_flow.cli','--source',str(source),'--output',str(out)],capture_output=True)
                self.assertEqual(result.returncode,2);self.assertEqual(out.read_text(),'prior')
            before=source.read_bytes()
            result=subprocess.run([sys.executable,'-m','supply_flow.cli','--source',str(source),'--output',str(source)],capture_output=True)
            self.assertEqual(result.returncode,2);self.assertEqual(source.read_bytes(),before)
if __name__=='__main__':unittest.main()
