from .test_helper import TestConfig
from luigi_bigquery import DatasetTarget, TableTarget

from unittest import TestCase
from nose.tools import eq_

import luigi

test_config = TestConfig(
    datasets = [
        {
            "datasetReference": {
                "datasetId": 'dataset_1',
                "projectId": 'test-project-id'
            }
        }
    ],

    tables = [
        {
            "tableReference": {
                "tableId": 'table_1',
                "datasetId": 'dataset_1',
                "projectId": 'test-project-id'
            },
            "numRows": 1
        },
        {
            "tableReference": {
                "tableId": 'table_2',
                "datasetId": 'dataset_1',
                "projectId": 'test-project-id'
            },
            "numRows": 0
        }
    ]
)

class DatasetTargetTestCase(TestCase):
    def setUp(self):
        test_config.setUp()

    def tearDown(self):
        test_config.tearDown()

    def test_exists(self):
        eq_(DatasetTarget('dataset_1', config=test_config).exists(), True)
        eq_(DatasetTarget('invalid_dataset_1', config=test_config).exists(), False)

class TableTargetTestCase(TestCase):
    def setUp(self):
        test_config.setUp()

    def tearDown(self):
        test_config.tearDown()

    def test_exists(self):
        eq_(TableTarget('dataset_1', 'table_1', config=test_config).exists(), True)
        eq_(TableTarget('dataset_1', 'invalid_table_1', config=test_config).exists(), False)

    def test_exists_check_empty(self):
        eq_(TableTarget('dataset_1', 'table_1', empty=True, config=test_config).exists(), False)
        eq_(TableTarget('dataset_1', 'table_2', empty=True, config=test_config).exists(), True)
