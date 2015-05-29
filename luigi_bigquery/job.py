class Job(object):
    def __init__(self, client, job_id):
        self.client = client
        self.id = job_id

    @property
    def job_id(self):
        return self.id

    @property
    def result_size(self):
        if not hasattr(self, '_result_size'):
            _, self._result_size = self.client.check_job(self.job_id)
        return self._result_size

    @property
    def schema(self):
        if not hasattr(self, '_schema'):
            self._schema = self.client.get_query_schema(self.job_id)
        return self._schema

    @property
    def result(self):
        if not hasattr(self, '_result'):
            self._result = self.client.get_query_rows(self.job_id)
        return self._result
