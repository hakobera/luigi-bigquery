from .test_helper import TestConfig
from luigi_bigquery import Query, ResultTarget

from unittest import TestCase
from nose.tools import eq_, raises

import luigi

test_config = TestConfig(
    jobs = [
        {
            'job_id': 1,
            'job_complete': True,
            'total_rows': 20,
            'schema': [{'name': 'cnt'}],
            'rows': [{'name': 5000}]
        }
    ]
)

class TestQuery(Query):
    config = test_config
    def query(self):
        return 'SELECT COUNT(1) cnt FROM www_access'

class QueryTestCase(TestCase):
    def setUp(self):
        test_config.setUp()

    def tearDown(self):
        test_config.tearDown()

    def test_simple(self):
        class SimpleTestQuery(TestQuery):
            pass
        task = SimpleTestQuery()
        task.run()

    def test_with_output(self):
        class OutputTestQuery(TestQuery):
            def output(self):
                return ResultTarget(test_config.get_tmp_path('{0}.job'.format(self)))
        task = OutputTestQuery()
        task.run()

    def test_with_dependency(self):
        class DependencyTestQuery(TestQuery):
            def output(self):
                return ResultTarget(test_config.get_tmp_path('{0}.job'.format(self)))

        class DependencyTestResult(luigi.Task):
            def requires(self):
                return DependencyTestQuery()

            def output(self):
                return LocalTarget(test_config.get_tmp_path('{0}.csv'.format(self)))

        task = DependencyTestResult()
        task.run()
