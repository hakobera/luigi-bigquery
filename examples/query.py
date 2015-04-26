import luigi
import luigi_bigquery

class MyQuery(luigi_bigquery.Query):
    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

    def output(self):
        return luigi.LocalTarget('data/MyQuery.csv')

    def run(self):
        result = self.run_query(self.query())
        with self.output().open('w') as f:
            result.to_csv(f)

if __name__ == '__main__':
    luigi.run()
