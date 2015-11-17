from luigi_bigquery.config import get_config
from luigi_bigquery.client import ResultProxy
from luigi_bigquery.job import Job
from luigi_bigquery.targets.result import ResultTarget
from luigi_bigquery.targets.bq import DatasetTarget
from luigi_bigquery.targets.bq import TableTarget
from luigi_bigquery.targets.gcs import BucketTarget
from luigi_bigquery.targets.gcs import FileTarget

import luigi
import jinja2
import time
import bigquery
import string
import random
import six

import logging
logger = logging.getLogger('luigi-interface')

def _id_generator(size=16, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

# Dataset

class DatasetTask(luigi.Task):
    config = get_config()
    dataset_id = luigi.Parameter()

    def output(self):
        return DatasetTarget(self.dataset_id)

    def run(self):
        client = self.config.get_client()
        logger.info('%s: creating dataset: %s', self, self.dataset_id)
        client.create_dataset(self.dataset_id)

        max_retry = 30
        retry = 0
        while True:
            time.sleep(5.0)
            if client.check_dataset(self.dataset_id):
                break
            retry += 1
            if retry > max_retry:
                msg = "DatasetTask(dataset_id={0}) max retry error.".format(self.dataset_id)
                logger.error(msg)
                raise Exception(msg)

# Table

class TableTask(luigi.Task):
    config = get_config()
    dataset_id = luigi.Parameter()
    table_id = luigi.Parameter()
    schema = luigi.Parameter(default=[], significant=False)
    empty = luigi.BooleanParameter(default=False, significant=False)

    def requires(self):
        return DatasetTask(self.dataset_id)

    def output(self):
        return TableTarget(self.dataset_id, self.table_id, empty=self.empty)

    def run(self):
        client = self.config.get_client()
        logger.info('%s: creating table: %s.%s', self, self.datasset_id, self.table_id)
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
            six.print_('-' * TERMINAL_WIDTH)
            six.print_('Query result:')
            six.print_(result.to_dataframe())
            six.print_('-' * TERMINAL_WIDTH)

class QueryTable(Query):
    create_disposition = bigquery.JOB_CREATE_IF_NEEDED
    write_disposition = bigquery.JOB_WRITE_EMPTY

    def requires(self):
        return DatasetTask(self.dataset())

    def output(self):
        return TableTarget(self.dataset(), self.table(), append=self._append())

    def dataset(self):
        return NotImplemented()

    def table(self):
        return NotImplemented()

    def _append(self):
        return self.write_disposition == bigquery.JOB_WRITE_APPEND

    def save_as_table(self, query):
        result = self.output()
        client = self.config.get_client()

        logger.info("%s: query: %s", self, query)
        job = client.write_to_table(
                query,
                dataset=self.dataset(),
                table=self.table(),
                create_disposition=self.create_disposition,
                write_disposition=self.write_disposition,
                allow_large_results=True)
        job_id = job['jobReference'].get('jobId')
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
        self.save_as_table(query)

class QueryToGCS(QueryTable):
    compression = luigi.Parameter(default='NONE') # or GZIP
    format = luigi.Parameter(default='CSV') # or NEWLINE_DELIMITED_JSON
    print_header = luigi.Parameter(default=True)
    use_temporary_table = luigi.Parameter(default=True)

    def __init__(self, *args, **kwargs):
        super(QueryToGCS, self).__init__(*args, **kwargs)
        self._random_id = 'tmp_{}'.format(_id_generator())

    def dataset(self):
        if self.use_temporary_table:
            return self._random_id
        else:
            return NotImplemented()

    def table(self):
        if self.use_temporary_table:
            return self._random_id
        else:
            return NotImplemented()

    def output(self):
        return FileTarget(self.bucket(), self.path())

    def bucket(self):
        return NotImplemented()

    def path(self):
        return NotImplemented()

    def export_to_gcs(self):
        result = self.output()
        client = self.config.get_client()

        logger.info("%s: export %s.%s to %s", self, self.dataset(), self.table(), result.uri())
        job = client.export_data_to_uris(
                destination_uris=[result.uri()],
                dataset=self.dataset(),
                table=self.table(),
                compression=self.compression,
                destination_format=self.format,
                print_header=self.print_header)
        job_id = job['jobReference'].get('jobId')
        logger.info("%s: bigquery.job.id: %s", self, job_id)

        try:
            job_resource = client.wait_for_job(job, timeout=3600)
        except:
            raise

    def _cleanup(self):
        if self.use_temporary_table:
            client = self.config.get_client()
            client.delete_dataset(self.dataset(), delete_contents=True)

    def run(self):
        query = self.load_query(self.source) if self.source else self.query()
        try:
            self.save_as_table(query)
            self.export_to_gcs()
        finally:
            self._cleanup()
