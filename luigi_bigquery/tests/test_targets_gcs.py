from .test_helper import TestConfig
from luigi_bigquery import FileTarget

from unittest import TestCase
from nose.tools import eq_

import luigi

test_config = TestConfig(
    objects = [
        {
            "bucket": "bucket",
            "name": "path/to/file.csv"
        }
    ]
)

class FileTargetTestCase(TestCase):
    def setUp(self):
        test_config.setUp()

    def tearDown(self):
        test_config.tearDown()

    def test_exists(self):
        eq_(FileTarget('bucket', 'path/to/file.csv', config=test_config).exists(), True)
        eq_(FileTarget('bucket', 'path/to/invalid.csv', config=test_config).exists(), False)
