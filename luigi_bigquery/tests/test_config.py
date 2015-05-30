from luigi_bigquery import Config, ConfigLoader, get_config

from unittest import TestCase
from nose.tools import eq_, raises
from bigquery import get_client as bqclient

import os
import luigi

class ConfigTestCase(TestCase):

    def test_create(self):
        config = Config('test-project-id', 'test-service-account', '/path/to/key.p12')
        eq_(config.project_id, 'test-project-id')
        eq_(config.service_account, 'test-service-account')
        eq_(config.private_key_file, '/path/to/key.p12')

class ConfigLoaderTestCase(TestCase):

    def setUp(self):
        self.environ = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.environ)
        if os.path.exists('client.cfg'):
            os.unlink('client.cfg')
            luigi.configuration.LuigiConfigParser._instance = None

    def _config(self, values):
        with open('client.cfg', 'w') as f:
            f.write("[bigquery]\n")
            for key, val in values.items():
                f.write("{0}: {1}\n".format(key, val))
        luigi.configuration.LuigiConfigParser._instance = None

    def _get_config(self):
        loader = ConfigLoader()
        loader.load_default()
        return loader.get_config()

    @raises(AssertionError)
    def test_no_config(self):
        config = self._get_config()
        eq_(config.project_id, None)
        config.get_client()

    def test_credentials_by_environ(self):
        os.environ['BQ_PROJECT_ID'] = 'test-project-id'
        os.environ['BQ_SERVICE_ACCOUNT'] = 'test-service-account'
        os.environ['BQ_PRIVATE_KEY_FILE'] = '/path/to/key.p12'
        config = self._get_config()
        eq_(config.project_id, 'test-project-id')
        eq_(config.service_account, 'test-service-account')
        eq_(config.private_key_file, '/path/to/key.p12')

    def test_credentials_by_luigi_config(self):
        self._config(
            {
                'project_id': 'test-project-id',
                'service_account': 'test-service-account',
                'private_key_file': '/path/to/key.p12',
            }
        )
        config = self._get_config()
        eq_(config.project_id, 'test-project-id')
        eq_(config.service_account, 'test-service-account')
        eq_(config.private_key_file, '/path/to/key.p12')

class GetConfigTestCase(TestCase):

    def test_default(self):
        config = get_config()
        eq_(type(config), Config)
