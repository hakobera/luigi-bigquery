from luigi_bigquery.config import get_config
from luigi_bigquery.client import ResultProxy

import luigi
import time

import logging
logger = logging.getLogger('luigi-interface')

# Query

class QueryTimeout(Exception):
    pass

class Query(luigi.Task):
    config = get_config()
    debug = False
    timeout = None

    def query(self):
        return NotImplemented()

    def run_query(self, query):
        result = self.output()
        client = self.config.get_client()
        job_id, _results = client.query(self.query())
        logger.info("%s: bigquery.job.id: %s", self, job_id)

        complete, row_count = client.check_job(job_id)
        try:
            if self.timeout:
                timeout = time.time() + self.timeout
            else:
                timeout = None

            while not complete:
                if timeout and time.time() > timeout:
                    raise QueryTimeout('{0} timed out'.format(self))
                time.sleep(5)
                complete, row_count = client.check_job(job_id)
        except:
            raise

        logger.info("%s: bigquery.job.result: job_id=%s row_count=%d", self, job_id, row_count)

        schema = client.get_query_schema(job_id)
        results = client.get_query_rows(job_id)
        return ResultProxy(job_id, schema, row_count, results)

    def run(self):
        query = self.query()
        result = self.run_query(query)

        if self.debug:
            import pandas as pd
            TERMINAL_WIDTH = 120
            pd.options.display.width = TERMINAL_WIDTH
            print '-' * TERMINAL_WIDTH
            print 'Query result:'
            print result.to_dataframe()
            print '-' * TERMINAL_WIDTH
