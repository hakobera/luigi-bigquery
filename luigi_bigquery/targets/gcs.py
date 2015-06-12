from luigi_bigquery.config import get_config

import luigi

import logging
logger = logging.getLogger('luigi-interface')

class BucketTarget(luigi.Target):
    def __init__(self, bucket_name, config=None):
        self.bucket_name = bucket_name
        self.config = config or get_config()

    def exists(self):
        client = self.config.get_gcs_client()
        return client.check_bucket(self.bucket_name)

class FileTarget(luigi.Target):
    def __init__(self, bucket_name, path, config=None):
        self.bucket_name = bucket_name
        if path[0] == '/':
            self.path = path[1:]
        else:
            self.path = path
        self.config = config or get_config()

    def exists(self):
        client = self.config.get_gcs_client()
        return client.check_file(self.bucket_name, self.path)

    def uri(self):
        return "gs://{0}/{1}".format(self.bucket_name, self.path)
