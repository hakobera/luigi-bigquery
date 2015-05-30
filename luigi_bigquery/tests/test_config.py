from luigi_bigquery import Config, get_config

from unittest import TestCase
from nose.tools import eq_, raises
from bigquery import get_client as bqclient

import os
import luigi

class ConfigTestCase(TestCase):

    def test_create(self):
        config = Config('project-id', 'service-email', '/path/to/key.p12')
        eq_(config.project_id, 'project-id')
        eq_(config.service_account, 'service-email')
        eq_(config.private_key_file, '/path/to/key.p12')

class GetConfigTestCase(TestCase):

    def test_default(self):
        config = get_config()
        eq_(type(config), Config)
