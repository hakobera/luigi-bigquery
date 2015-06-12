from luigi_bigquery import ResultProxy

import os
import shutil
import tempfile

class MockClient(object):
    def __init__(self, datasets, tables, jobs):
        self._datasets = datasets
        self._tables = tables
        self._jobs = jobs

    def create_dataset(self, dataset_id, friendly_name=None, description=None, access=None):
        dataset_data = _dataset_resource(dataset_id, friendly_name, description, access)
        self._datasets.append(dataset_data)
        return dataset_data

    def get_datasets(self):
        return self._datasets

    def get_table(self, dataset_id, table_id):
        for table in self._tables:
            ref = table['tableReference']
            if ref['datasetId'] == dataset_id and ref['tableId'] == table_id:
                return table
        return {}

    def delete_table(self, dataset_id, table_id):
        pass

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

    def _dataset_resource(self, dataset_id, friendly_name=None, description=None, access=None):
        data = {
            "datasetReference": {
                "datasetId": dataset_id,
                "projectId": 'test-project-id'
            }
        }
        if friendly_name:
            data["friendlyName"] = friendly_name
        if description:
            data["description"] = description
        if access:
            data["access"] = access

        return data


class MockGCSClient(object):

    def __init__(self, objects):
        self._objects = objects

    def get_file(self, bucket_name, path):
        for obj in self._objects:
            if obj['bucket'] == bucket_name and obj['name'] == path:
                return obj
        return {}

    def check_file(self, bucket_name, path):
        file = self.get_file(bucket_name, path)
        return bool(file)

class TestConfig(object):
    def __init__(self, datasets=[], tables=[], jobs=[], objects=[]):
        self.datasets = datasets
        self.tables = tables
        self.objects = objects
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
        return MockClient(datasets=self.datasets, tables=self.tables, jobs=self._jobs)

    def get_gcs_client(self):
        return MockGCSClient(objects=self.objects)
