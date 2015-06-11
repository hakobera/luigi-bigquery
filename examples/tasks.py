import luigi
import luigi_bigquery

## Running Queries

class MyQuery(luigi_bigquery.Query):

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

## Getting Results

class MyQueryRun(luigi_bigquery.Query):

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

    def run(self):
        result = self.run_query(self.query())
        print "Job ID     :", result.job_id
        print "Result size:", result.size
        print "Result     :"
        print "\t".join([c['name'] for i, c in result.description])
        print "----"
        for row in result:
            print "\t".join([str(c) for c in row])
        print '===================='

class MyQuerySave(luigi_bigquery.Query):

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

    def output(self):
        return luigi.LocalTarget('MyQuerySave.csv')

    def run(self):
        result = self.run_query(self.query())
        with self.output().open('w') as f:
            result.to_csv(f)

## Building Pipelines

class MyQueryStep1(luigi_bigquery.Query):

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

    def output(self):
        return luigi_bigquery.ResultTarget('MyQueryStep1.job')

class MyQueryStep2(luigi.Task):
    def requires(self):
        return MyQueryStep1()

    def output(self):
        return luigi.LocalTarget('MyQueryStep2.csv')

    def run(self):
        # retrieve the result and save it as a CSV file
        with self.output().open('w') as f:
            self.input().result.to_csv(f)

class MyQueryStep3(luigi.Task):
    def requires(self):
        return MyQueryStep2()

    def output(self):
        return luigi.LocalTarget('MyQueryStep3.txt')

    def run(self):
        with self.input().open() as f:
            # process the result here
            print f.read()
        with self.output().open('w') as f:
            # crate the final output
            f.write('done')

## Templating Queries

class MyQueryFromTemplate(luigi_bigquery.Query):
    source = 'templates/query_with_language.sql'

    # variables used in the template
    language = 'Python'

class MuQueryWithVariables(luigi_bigquery.Query):
    source = 'templates/query_with_variables.sql'

    # define variables
    variables = {
        'language': 'Python',
    }

    # or use property for dynamic variables
    # @property
    # def variables(self):
    #     return {
    #         'language': 'Python',
    #     }

## Passing Parameters

class MyQueryWithParameters(luigi_bigquery.Query):
    source = 'templates/query_with_time_range.sql'

    # parameters
    year = luigi.IntParameter()

    def output(self):
        # create a unique name for this output using parameters
        return luigi_bigquery.ResultTarget('MyQueryWithParameters-{0}.job'.format(self.year))

class MyQueryAggregator(luigi.Task):

    def requires(self):
        # create a list of tasks with different parameters
        return [
            MyQueryWithParameters(2009),
            MyQueryWithParameters(2010),
            MyQueryWithParameters(2011),
            MyQueryWithParameters(2012)
        ]

    def output(self):
        return luigi.LocalTarget('MyQueryAggregator.txt')

    def run(self):
        with self.output().open('w') as f:
            # repeat for each ResultTarget
            for target in self.input():
                # output results into a single file
                for row in target.result:
                    f.write(str(row) + "\n")

## Building Pipelines using QueryTable

class MyQueryTableStep1(luigi_bigquery.QueryTable):

    def dataset(self):
        return 'tmp'

    def table(self):
        return 'github_nested_count'

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

class MyQueryTableStep2(luigi_bigquery.Query):
    def requires(self):
        return MyQueryTableStep1()

    def query(self):
        input = self.input()
        print(input.dataset_id)
        print(input.table_id)
        return "SELECT cnt FROM [{0}.{1}]".format(input.dataset_id, input.table_id)

    def output(self):
        return luigi.LocalTarget('MyQueryTableStep2.csv')

    def run(self):
        # retrieve the result and save it as a CSV file
        result = self.run_query(self.query())
        with self.output().open('w') as f:
            result.to_csv(f)

if __name__ == '__main__':
    luigi.run()
import luigi
import luigi_bigquery

## Running Queries

class MyQuery(luigi_bigquery.Query):

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

## Getting Results

class MyQueryRun(luigi_bigquery.Query):

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

    def run(self):
        result = self.run_query(self.query())
        print "Job ID     :", result.job_id
        print "Result size:", result.size
        print "Result     :"
        print "\t".join([c['name'] for i, c in result.description])
        print "----"
        for row in result:
            print "\t".join([str(c) for c in row])
        print '===================='

class MyQuerySave(luigi_bigquery.Query):

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

    def output(self):
        return luigi.LocalTarget('MyQuerySave.csv')

    def run(self):
        result = self.run_query(self.query())
        with self.output().open('w') as f:
            result.to_csv(f)

## Building Pipelines

class MyQueryStep1(luigi_bigquery.Query):

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

    def output(self):
        return luigi_bigquery.ResultTarget('MyQueryStep1.job')

class MyQueryStep2(luigi.Task):
    def requires(self):
        return MyQueryStep1()

    def output(self):
        return luigi.LocalTarget('MyQueryStep2.csv')

    def run(self):
        # retrieve the result and save it as a CSV file
        with self.output().open('w') as f:
            self.input().result.to_csv(f)

class MyQueryStep3(luigi.Task):
    def requires(self):
        return MyQueryStep2()

    def output(self):
        return luigi.LocalTarget('MyQueryStep3.txt')

    def run(self):
        with self.input().open() as f:
            # process the result here
            print f.read()
        with self.output().open('w') as f:
            # crate the final output
            f.write('done')

## Templating Queries

class MyQueryFromTemplate(luigi_bigquery.Query):
    source = 'templates/query_with_language.sql'

    # variables used in the template
    language = 'Python'

class MuQueryWithVariables(luigi_bigquery.Query):
    source = 'templates/query_with_variables.sql'

    # define variables
    variables = {
        'language': 'Python',
    }

    # or use property for dynamic variables
    # @property
    # def variables(self):
    #     return {
    #         'language': 'Python',
    #     }

## Passing Parameters

class MyQueryWithParameters(luigi_bigquery.Query):
    source = 'templates/query_with_time_range.sql'

    # parameters
    year = luigi.IntParameter()

    def output(self):
        # create a unique name for this output using parameters
        return luigi_bigquery.ResultTarget('MyQueryWithParameters-{0}.job'.format(self.year))

class MyQueryAggregator(luigi.Task):

    def requires(self):
        # create a list of tasks with different parameters
        return [
            MyQueryWithParameters(2009),
            MyQueryWithParameters(2010),
            MyQueryWithParameters(2011),
            MyQueryWithParameters(2012)
        ]

    def output(self):
        return luigi.LocalTarget('MyQueryAggregator.txt')

    def run(self):
        with self.output().open('w') as f:
            # repeat for each ResultTarget
            for target in self.input():
                # output results into a single file
                for row in target.result:
                    f.write(str(row) + "\n")

## Building Pipelines using QueryTable

class MyQueryTableStep1(luigi_bigquery.QueryTable):

    def dataset(self):
        return 'tmp'

    def table(self):
        return 'github_nested_count'

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

class MyQueryTableStep2(luigi_bigquery.Query):
    def requires(self):
        return MyQueryTableStep1()

    def query(self):
        input = self.input()
        return "SELECT cnt FROM [{0}.{1}]".format(input.dataset_id, input.table_id)

    def output(self):
        return luigi.LocalTarget('MyQueryTableStep2.csv')

    def run(self):
        # retrieve the result and save it as a CSV file
        result = self.run_query(self.query())
        with self.output().open('w') as f:
            result.to_csv(f)

# QueryToGCS

class MyQueryToGCS(luigi_bigquery.QueryToGCS):
    use_temporary_table = True

    def bucket(self):
        return 'my-bucket'

    def path(self):
        return '/path/to/file.csv'

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

if __name__ == '__main__':
    luigi.run()
