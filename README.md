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
(in `s3://udacity-dend/log_data/`) and JSON metadata about the songs the users listen to
(`s3://udacity-dend/song_data/`).

## Amazon Redshift

Follow the instructions [here](resources/docs/aws_redshift.md).

## Airflow

Follow the instructions [here](resources/docs/airflow.md).
