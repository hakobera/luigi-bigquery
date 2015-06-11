from luigi_bigquery.config import get_config

import luigi

import logging
logger = logging.getLogger('luigi-interface')

class SchemaError(Exception):
    pass

class DatasetTarget(luigi.Target):
    def __init__(self, dataset_id, config=None):
        self.dataset_id = dataset_id
        self.config = config or get_config()

    def exists(self):
        client = self.config.get_client()
        return self.dataset_id in [ds['datasetReference']['datasetId'] for ds in client.get_datasets()]

class TableTarget(luigi.Target):
    def __init__(self, dataset_id, table_id, schema=None, empty=False, config=None, append=False):
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema = schema or []
        self.empty = empty
        self.config = config or get_config()

    def exists(self):
        client = self.config.get_client()
        table = client.get_table(self.dataset_id, self.table_id)

        if not bool(table) or self.append:
            return False

        count = table.get('numRows', 0)

        if self.empty:
            if count == 0:
                return True
            else:
                logger.info('Deleting table: %s.%s', self.dataset_id, self.table_id)
                client.delete_table(self.dataset_id, self.table_id)
                return False
        else:
            return True
