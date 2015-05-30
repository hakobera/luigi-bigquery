from luigi_bigquery import ResultProxy

import os
import shutil
import tempfile

class MockClient(object):
    def __init__(self, tables, jobs):
        self._tables = tables
        self._jobs = jobs

    def check_job(self, job_id):
        job = self._job(job_id)
        return (job.get('job_complete', False), int(job.get('total_rows', 0)))

    def get_query_schema(self, job_id):
        job = self._job(job_id)
        return job['schema']

    def get_query_rows(self, job_id):
        job = self._job(job_id)
        return job['rows']

    def query(self, query):
        return (self._jobs[0]['job_id'], None)

    def _job(self, job_id):
        for job in self._jobs:
            if job['job_id'] == job_id:
                return job
        return {}

class TestConfig(object):
    def __init__(self, tables=[], jobs=[]):
        self.tables = tables
        self._jobs = jobs
        self.tmp_dir = None

    def setUp(self):
        if not self.tmp_dir:
            self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmp_dir:
            shutil.rmtree(self.tmp_dir)
            self.tmp_dir = None

    def get_tmp_path(self, filename):
        return os.path.join(self.tmp_dir, filename)

    def get_client(self):
        return MockClient(tables=self, jobs=self._jobs)
