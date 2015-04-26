class ResultProxy(object):
    def __init__(self, job_id, schema, row_count, results):
        self.id = job_id
        self.schema = schema
        self.row_count = row_count
        self.results = results

    @property
    def job_id(self):
        return self.id

    @property
    def size(self):
        return self.row_count

    @property
    def description(self):
        return enumerate(self.schema)

    def _columns(self):
        return [c['name'] for i, c in self.description]

    def _rows(self):
        rows = []
        for row in self.results:
            rows.append([row[c] for c in self._columns()])
        return rows

    def to_csv(self, path_or_file):
        def _to_csv(f):
            f.write(",".join(self._columns()))
            f.write("\n")
            for row in self._rows():
                f.write(",".join([str(c) if c else '' for c in row]) + "\n")

        if type(path_or_file) in [str, unicode]:
            with file(path_or_file, 'w') as f:
                return _to_csv(f)
        else:
            return _to_csv(path_or_file)

    def to_dataframe(self):
        import pandas as pd
        return pd.DataFrame(self.results, columns=self._columns())
