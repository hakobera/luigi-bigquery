import os
import luigi
from bigquery import get_client as bqclient
from .gcs import get_gcs_client

import logging
logger = logging.getLogger('luigi-interface')

class Config(object):
    def __init__(self, project_id, service_account, private_key_file):
        self.project_id = project_id
        self.service_account = service_account
        self.private_key_file = private_key_file

    def get_client(self):
        return bqclient(
                self.project_id,
                service_account=self.service_account,
                private_key_file=self.private_key_file,
                readonly=False)

    def get_gcs_client(self):
        return get_gcs_client(
                self.project_id,
                service_account=self.service_account,
                private_key_file=self.private_key_file)

class ConfigLoader(object):
    _instance = None

    @classmethod
    def instance(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
            cls._instance.load_default()
        return cls._instance

    def __init__(self):
        self.config = None

    def get_config(self):
        return self.config

    def load_default(self):
        luigi_config = luigi.configuration.get_config()
        project_id = luigi_config.get('bigquery', 'project_id', os.environ.get('BQ_PROJECT_ID'))
        service_account = luigi_config.get('bigquery', 'service_account', os.environ.get('BQ_SERVICE_ACCOUNT'))
        private_key_file = luigi_config.get('bigquery', 'private_key_file', os.environ.get('BQ_PRIVATE_KEY_FILE'))
        self.config = Config(project_id, service_account, private_key_file)

def get_config():
    return ConfigLoader.instance().get_config()
