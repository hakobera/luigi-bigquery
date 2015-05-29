import luigi
from luigi_bigquery.config import get_config
from luigi_bigquery.client import ResultProxy
from luigi_bigquery.job import Job

import json
import os

import logging
logger = logging.getLogger('luigi-interface')

class ResultTarget(luigi.Target):
    def __init__(self, path, config=None):
        self.path = path
        self.config = config or get_config()

    # Job result handling

    def save_result_state(self, result):
        state_dir = os.path.dirname(self.path)
        if state_dir != '' and not os.path.exists(state_dir):
            os.makedirs(state_dir)
        with open(self.path, 'w') as f:
            state = {'job_id': result.job_id}
            json.dump(state, f)

    def load_result_state(self):
        with open(self.path) as f:
            return json.load(f)

    @property
    def job_id(self):
        return self.load_result_state()['job_id']

    @property
    def result(self):
        if not hasattr(self, '_result'):
            client = self.config.get_client()
            self._result = ResultProxy(Job(client, self.job_id))
        return self._result

    # Luigi support

    def exists(self):
        if not os.path.exists(self.path):
            return False

        client = self.config.get_client()
        complete, _ = client.check_job(self.job_id)
        return complete
