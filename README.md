# Data Pipelines with Airflow

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and
monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to
achieve this is Apache Airflow.

They expect:

- high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and
allow easy backfills;
- tests against their datasets after the ETL steps have been executed to catch any discrepancies in
the datasets, since the data quality plays a big part when analyses are executed on top the data
warehouse.

## Datasets

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon
Redshift. The source datasets consist of JSON logs that tell about user activity in the application
(in `s3://udacity-dend/log_data`) and JSON metadata about the songs the users listen to
(`s3://udacity-dend/song_data`).

## Airflow

### Set up and run

Setup:

```bash
# Create conda environment
conda create -yn etl-airflow python=3.6
conda activate etl-airflow

# Set Airflow home (optional; default: ~/airflow)
export AIRFLOW_HOME=~/airflow

# Install
python -m pip install apache-airflow

# Initialize the database
airflow initdb
```

Airflow UI:

```bash
conda activate etl-airflow

# Start the web server, default port is 8080
airflow webserver -p 8080
```

Scheduler (in another terminal):

```bash
conda activate etl-airflow

# Start the scheduler
airflow scheduler
```

To open the airflow UI, visit <localhost:8080> in the browser (preferably Google Chrome, because
other browsers occasionally have issues rendering the Airflow UI),

### Add connection to Redshift

1. To go to the Airflow UI
1. Menu "Admin" --> "Connections" --> "Create"
1. Enter the AWS credentials:
   - `Conn Id`: "aws_credentials"
   - `Conn Type`: "Amazon Web Services"
   - `Login`: enter the AWS access key ID
   - `Password`: enter the AWS secret access key
1. "Save and Add Another"
1. Enter the AWS Redshift cluster properties (they can be obtained from the [cluster page in the AWS
console](https://console.aws.amazon.com/redshift/)):
   - `Conn Id`: "redshift"
   - `Conn Type`: "Postgres"
   - `Host`: enter the endpoint of the Redshift cluster, excluding the port at the end (e.g.,
   `<cluster-name>.<cluster-id>.<region>.redshift.amazonaws.com`)
   - `Schema`: "dev"
   - `Login`: "awsuser"
   - `Password`: enter the password created when launching the Redshift cluster
   - `Port`: 5439

### Configure the DAG

In the DAG, the `default_args` are set according to these guidelines:

- The DAG does not have dependencies on past runs
- On failure, the task are retried 3 times
- Retries happen every 5 minutes
- Catchup is turned off
- Do not email on retry

The graph view follows the flow below:
![image](resources/img/example-dag.png)

### Operators

Four different operators were created to:

1. stage the data;
1. transform the data for the dimension tables;
1. transform the data for the fact table;
1. run checks on data quality.

#### Stage Operator

The stage operator loads any JSON formatted files from S3 to Amazon Redshift. The operator creates
and runs a SQL COPY statement based on the parameters provided (they specify where in S3 the file is
loaded and what is the target table). It also contains a templated field that allows it to load
timestamped files from S3 based on the execution time and run backfills.

#### Fact and Dimension Operators

Dimension and fact operators run SQL queries to perform data transformations. They take as input a
SQL statement and target database on which to run the query against. A target table that will
contain the results of the transformation.

Dimension loads are often done with the truncate-insert pattern, where the target table is emptied
before the load. Thus, it is also possible to have a parameter that allows switching between insert
modes when loading dimensions. Fact tables are usually so massive that they should only allow append
type functionality.

#### Data Quality Operator

The data quality operator is used to run checks on the data itself. It receives one or more
SQL-based test cases along with the expected results and execute the tests. For each the test, the
test result and expected result are checked and, if there is no match, the operator raises an
exception and the task should retry and fail eventually.

For example, one test could be a SQL statement that checks if certain column contains NULL values by
counting all the rows that have NULL in the column. We do not want to have any NULLs so expected
result would be 0 and the test would compare the SQL statement's outcome to the expected result.
