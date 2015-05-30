from luigi_bigquery.config import get_config
from luigi_bigquery.client import ResultProxy
from luigi_bigquery.job import Job
from luigi_bigquery.targets.result import ResultTarget
from luigi_bigquery.targets.bq import DatasetTarget
from luigi_bigquery.targets.bq import TableTarget

import luigi
import jinja2
import time

import logging
logger = logging.getLogger('luigi-interface')

# Dataset

class DatasetTask(luigi.Task):
    config = get_config()
    dataset_id = luigi.Parameter()

    def output(self):
        return DatasetTarget(self.dataset_id)

    def run(self):
        client = self.config.get_client()
        logger.debug('%s: creating dataset: %s', self, self.dataset_id)
        client.create_dateset(self.dataset_id)

# Table

class TableTask(luigi.Task):
    config = get_config()
    dataset_id = luigi.Parameter()
    table_id = luigi.Parameter()
    schema = luigi.Parameter(is_list=True, default=[], significant=False)
    empty = luigi.BooleanParameter(default=False, significant=False)

    def requires(self):
        return DatasetTask(self.dataset_id)

    def output(self):
        return TableTarget(self.dataset_id, self.table_id, self.schema, empty=self.empty)

    def run(self):
        client = self.config.get_client()
        logger.debug('%s: creating table: %s.%s', self, self.datasset_id, self.table_id)
        client.create_table(self.dataset_id, self.table_id, self.schema)

# Query

class QueryTimeout(Exception):
    pass

class Query(luigi.Task):
    config = get_config()
    debug = False
    timeout = 3600
    source = None
    variables = {}

    def query(self):
        return NotImplemented()

    def load_query(self, source):
        env = jinja2.Environment(loader=jinja2.PackageLoader(self.__module__, '.'))
        template = env.get_template(source)
        return template.render(task=self, **self.variables)

    def run_query(self, query):
        result = self.output()
        client = self.config.get_client()

        logger.info("%s: query: %s", self, query)
        job_id, _ = client.query(query)
        logger.info("%s: bigquery.job.id: %s", self, job_id)

        complete, result_size = client.check_job(job_id)
        try:
            if self.timeout:
                timeout = time.time() + self.timeout
            else:
                timeout = None

            while not complete:
                if timeout and time.time() > timeout:
                    raise QueryTimeout('{0} timed out'.format(self))
                time.sleep(5)
                complete, result_size = client.check_job(job_id)
        except:
            raise

        logger.info("%s: bigquery.job.result: job_id=%s result_size=%d", self, job_id, result_size)

        return ResultProxy(Job(client, job_id))

    def run(self):
        query = self.load_query(self.source) if self.source else self.query()
        result = self.run_query(query)
        target = self.output()

        if target and isinstance(target, ResultTarget):
            target.save_result_state(result)

        if self.debug:
            import pandas as pd
            TERMINAL_WIDTH = 120
            pd.options.display.width = TERMINAL_WIDTH
            print '-' * TERMINAL_WIDTH
            print 'Query result:'
            print result.to_dataframe()
            print '-' * TERMINAL_WIDTH
