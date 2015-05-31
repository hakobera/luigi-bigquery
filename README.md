# Luigi-BigQuery

[![Build Status](https://travis-ci.org/hakobera/luigi-bigquery.svg?branch=master)](https://travis-ci.org/hakobera/luigi-bigquery)

Luigi integration for Google BigQuery.

**CAUTION: This module is currently under active development.**

## Prerequisities

- Python == 2.7 (Not supported Python >= 3.0)
- [luigi](https://github.com/spotify/luigi) >= 1.1.2

## Install

You can install Luigi-BigQuery using pip:

```sh
$ pip install git+https://github.com/hakobera/luigi-bigquery#egg=luigi=bigquery
```

## Configuration

You can set your `project_id`, `service_account` and `private_key_file` as an environment variables `BQ_PROJECT_ID`, `BQ_SERVICE_ACCOUNT` and `BQ_PRIVATE_KEY_FILE`:

```sh
$ export BQ_PROJECT_ID=your-project-id
$ export BQ_SERVICE_ACCOUNT=your-service-account
$ export BQ_PRIVATE_KEY_FILE=/path/to/key.p12
```

Alternatively, you can use Luigi configuration file (`./client.cfg` or `/etc/lugi/client.cfg`):

```
# configuration for Luigi
[core]
error-email: you@example.com

# configuration for Luigi-BigQuery
[bigquery]
project_id: your_project_id
service_account: your_service_account
private_key_file: /path/to/key.p12
```

## Running Queries

Queries are defined as subclasses of `luigi_bigquery.Query`

```python
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
```

You can submit your query as a normal Python script as follows:

```
$ python myquery.py MyQuery --local-scheduler
DEBUG: Checking if MyQuery() is complete
INFO: Scheduled MyQuery() (PENDING)
INFO: Done scheduling tasks
INFO: Running Worker with 1 processes
DEBUG: Asking scheduler for work...
DEBUG: Pending tasks: 1
INFO: [pid 1234] Worker Worker(salt=1234, workers=1, host=...) running   MyQuery()
INFO: MyQuery(): bigquery.job.id: job_1234
INFO: MyQuery(): bigquery.job.result: job_id=job_1234 row_count=1
INFO: [pid 1234] Worker Worker(salt=1234, workers=1, host=...) done      MyQuery()
DEBUG: 1 running tasks, waiting for next task to finish
DEBUG: Asking scheduler for work...
INFO: Done
INFO: There are no more tasks to run at this time
INFO: Worker Worker(salt=1234, workers=1, host=...) was stopped. Shutting down Keep-Alive thread
```

You can see the query result in file `data/MyQuery.csv`

```
$ cat data/MyQuery.csv
cnt
2541639
```

## For more examples

See [examples/tasks.py](./examples/tasks.py)

## License

Apache License Version 2.0
