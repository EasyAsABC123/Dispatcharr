import ast
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock
import workload

class SustainedControls(unittest.TestCase):
    def collect(self,flag,close_error=False):
        response=mock.Mock(status=200)
        if close_error:response.close.side_effect=OSError('held close failure')
        connection=mock.Mock();connection.getresponse.return_value=response
        stop=threading.Event();stop.set();shared={'stop':stop,'release':threading.Event(),'prefix_limit':1024,'physical_close_required':flag}
        with tempfile.TemporaryDirectory()as directory,mock.patch.object(workload.http.client,'HTTPConnection',return_value=connection):
            if close_error:
                with self.assertRaises(OSError):workload.reader('http://owned.invalid','/stream','fixture-only',shared,threading.Event(),Path(directory)/'prefix',1)
                return
            row=workload.reader('http://owned.invalid','/stream','fixture-only',shared,threading.Event(),Path(directory)/'prefix',1)
        return row,response,connection
    def test_explicit_response_and_connection_closed_before_receipt(self):
        row,response,connection=self.collect(True);response.close.assert_called_once();connection.close.assert_called_once();self.assertTrue(row['physical_socket_closed'])
    def test_old_default_does_not_invent_physical_receipt(self):
        row,response,connection=self.collect(False);response.close.assert_not_called();connection.close.assert_called_once();self.assertNotIn('physical_socket_closed',row)
    def test_failed_close_does_not_emit_true_receipt(self):self.collect(True,True)
    def test_Source_only_fixed15_60_no_new_observer(self):
        text=Path(__file__).with_name('sustained.py').read_text();self.assertIn("phases=[(1,1,'source')]",text);self.assertIn('token,viewers,15,60,phaseout',text);self.assertNotIn('Window(',text);self.assertNotIn('from resources import Window',text);self.assertNotIn('build-manifest',text)
    def test_retention_after_physical_join_separate_from_active(self):
        text=Path(__file__).with_name('sustained.py').read_text();self.assertIn("closed_at=time.monotonic();snapshot('post-physical-close')",text);self.assertIn('closed_at+70',text);self.assertIn("snapshot('retention-settled')",text)

if __name__=='__main__':unittest.main()
