import unittest
import click
import click.testing
import json
from main import process_event
from utils import load_event

class TestPagView(unittest.TestCase):
    JOB_PATH='./job.yaml'
    SUCCESS_PATH='./test/success/db.pageView.txt'
    UNSUCCESS_PATH='./test/fail/db.event_error_log.txt'

    def test_on_success(self):

        event = load_event('./test/testcases/test_pageview_success.json')
        process_event(self.__class__.JOB_PATH, event)
        latest_record = self._load_latest_record(self.__class__.SUCCESS_PATH)
        self.assertEqual(latest_record, json.dumps(json.loads(event)))

    def test_on_fail_invalid_format(self):

        event = load_event('./test/testcases/test_pageview_fail_invalid_format.json')
        with self.assertRaises(json.JSONDecodeError):
            process_event(self.__class__.JOB_PATH, event)
        

    def test_on_fail_wrong_dest(self):

        event = load_event('./test/testcases/test_pageview_fail_wrong_dest.json')
        process_event(self.__class__.JOB_PATH, event)
        latest_record = self._load_latest_record(self.__class__.UNSUCCESS_PATH)
        self.assertEqual(latest_record, json.dumps(json.loads(event)))

    def test_on_fail_wrong_payloads(self):

        event = load_event('./test/testcases/test_pageview_fail_wrong_payloads.json')
        process_event(self.__class__.JOB_PATH, event)
        latest_record = self._load_latest_record(self.__class__.UNSUCCESS_PATH)
        self.assertEqual(latest_record, json.dumps(json.loads(event)))

    def test_on_fail_wrong_timestamp(self):

        event = load_event('./test/testcases/test_pageview_fail_wrong_timestamp.json')
        process_event(self.__class__.JOB_PATH, event)
        latest_record = self._load_latest_record(self.__class__.UNSUCCESS_PATH)
        self.assertEqual(latest_record, json.dumps(json.loads(event)))

    def test_on_fail_wrong_userid(self):

        event = load_event('./test/testcases/test_pageview_fail_wrong_userid.json')
        process_event(self.__class__.JOB_PATH, event)
        latest_record = self._load_latest_record(self.__class__.UNSUCCESS_PATH)
        self.assertEqual(latest_record, json.dumps(json.loads(event)))
    
    def _load_latest_record(self, path):
        with open(path, 'r') as f:
            return f.readlines()[-1].rstrip()

if __name__=='__main__':
    unittest.main()