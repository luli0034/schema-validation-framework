import unittest
import click
import click.testing
import json
from main import process_event
from utils import load_event

class TestRegister(unittest.TestCase):
    JOB_PATH='./job.yaml'
    SUCCESS_PATH='./test/success/db.Register.txt'
    UNSUCCESS_PATH='./test/fail/db.event_error_log.txt'

    def test_on_success(self):

        event = load_event('./test/testcases/test_register_success.json')
        process_event(self.__class__.JOB_PATH, event)
        latest_record = self._load_latest_record(self.__class__.SUCCESS_PATH)
        self.assertEqual(latest_record, json.dumps(json.loads(event)))

    def test_on_fail_wrong_enum(self):
        
        event = load_event('./test/testcases/test_register_fail_wrong_enum.json')
        process_event(self.__class__.JOB_PATH, event)
        latest_record = self._load_latest_record(self.__class__.UNSUCCESS_PATH)
        self.assertEqual(latest_record, json.dumps(json.loads(event)))

    def _load_latest_record(self, path):
        with open(path, 'r') as f:
            return f.readlines()[-1].rstrip()

if __name__=='__main__':
    unittest.main()