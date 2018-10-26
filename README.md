# DataflowTemplates
Useful Cloud Dataflow custom templates.

You should use [Google provided templates](https://github.com/GoogleCloudPlatform/DataflowTemplates) if your use case fits.
This templates target use cases that official templates do not cover.

## Template Pipelines

* [Spanner to GCS Text](src/main/java/net/orfeon/cloud/dataflow/templates/SpannerToText.java)
* [Spanner to GCS Avro](src/main/java/net/orfeon/cloud/dataflow/templates/SpannerToAvro.java)
* [GCS Avro to Spanner](src/main/java/net/orfeon/cloud/dataflow/templates/AvroToSpanner.java)
* [Spanner to BigQuery](src/main/java/net/orfeon/cloud/dataflow/templates/SpannerToBigQuery.java)

## Getting Started

### Requirements

* Java 8
* Maven 3

### Creating a Template File

Dataflow templates can be [created](https://cloud.google.com/dataflow/docs/templates/creating-templates#creating-and-staging-templates)
using a maven command which builds the project and stages the template
file on Google Cloud Storage. Any parameters passed at template build
time will not be able to be overwritten at execution time.

```sh
mvn compile exec:java \
-Pdataflow-runner \
-Dexec.mainClass=net.orfeon.cloud.dataflow.templates.<template-class> \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=<project-id> \
--stagingLocation=gs://<bucket-name>/staging \
--tempLocation=gs://<bucket-name>/temp \
--templateLocation=gs://<bucket-name>/templates/<template-name>.json \
--runner=DataflowRunner"
```

### Executing a Template File

Once the template is staged on Google Cloud Storage, it can then be
executed using the
[gcloud CLI](https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run)
tool. The runtime parameters required by the template can be passed in the
parameters field via comma-separated list of `paramName=Value`.

```sh
gcloud dataflow jobs run <job-name> \
--gcs-location=<template-location> \
--zone=<zone> \
--parameters <parameters>
```

## Template Parameters

### SpannerToText

SpannerToText's feature is that user can specify sql to extract record as template parameter.
Because of template parameter, user can extract record flexibly using template such as daily differential backup from Spanner.

SpannerToText support json and csv format, and support partition query and single query.
These settings must be reflected as different template pipeline.

You can deploy each template pipeline with parameters '--type' and '--single'.

Parameter '--type' should be set 'json' or 'csv' (default is 'json').
Parameter '--single' should be set true or false (default is false).

| Parameter       | Type   | Description                                      |
|-----------------|--------|--------------------------------------------------|
| projectId       | String | projectID for Spanner you will read.             |
| instanceId      | String | Spanner instanceID you will read.                |
| databaseId      | String | Spanner databaseID you will read.                |
| query           | String | SQL query to read record from Spanner            |
| output          | String | GCS path to output. prefix must start with gs:// |
| timestampBound  | String | (Optional) timestamp bound (format: yyyy-MM-ddTHH:mm:SSZ). default is strong.   |

* Query must be root partitionable, that must have a DistributedUnion at the root.
For example, query that includes 'order by', 'limit' operation can not have DistributedUnion at the root.
Please run EXPLAIN for query plan details before running template.
* If you want to use non partitionable query, use single query template pipeline (set option --single=true when deploying template).
* [timestampBound](https://cloud.google.com/spanner/docs/timestamp-bounds) must be within one hour.
* timestampBound format example in japan: '2018-10-01T18:00:00+09:00'.

### SpannerToAvro

SpannerToAvro's feature is that user can specify sql to extract record as template parameter.
You can restore Spanner table, BigQuery table from avro files you outputted.
Template parameters are same as SpannerToText.

You can deploy each template pipeline with parameters '--single'.

Parameter '--single' should be set true or false (default is false).

| Parameter       | Type   | Description                                      |
|-----------------|--------|--------------------------------------------------|
| projectId       | String | projectID for Spanner you will read.             |
| instanceId      | String | Spanner instanceID you will read.                |
| databaseId      | String | Spanner databaseID you will read.                |
| query           | String | SQL query to read record from Spanner            |
| output          | String | GCS path to output. prefix must start with gs:// |
| fieldKey        | String | (Optional) Query result field to store avro file separately |
| timestampBound  | String | (Optional) timestamp bound (format: yyyy-MM-ddTHH:mm:SSZ). default is strong.   |

* Some spanner data type will be converted. Date will be converted to epoch days (Int32), Timestamp will be converted to epoch milliseconds (Int64).


### AvroToSpanner

AvroToSpanner recovers Spanner table from avro files you made using SpannerToAvro template.
Template parameters are same as SpannerToText.

| Parameter       | Type   | Description                                      |
|-----------------|--------|--------------------------------------------------|
| projectId       | String | projectID for Spanner you will recover.          |
| instanceId      | String | Spanner instanceID you will recover.             |
| databaseId      | String | Spanner databaseID you will recover.             |
| table           | String | Spanner table name to insert records.            |
| input           | String | GCS path for avro files. prefix must start with gs:// |

### SpannerToBigQuery

SpannerToBigQuery's feature is that user can specify sql to extract record as template parameter.
Template parameters are same as SpannerToText.
BigQuery destination table must be created.

| Parameter       | Type   | Description                                      |
|-----------------|--------|--------------------------------------------------|
| projectId       | String | projectID for Spanner you will read.             |
| instanceId      | String | Spanner instanceID you will read.                |
| databaseId      | String | Spanner databaseID you will read.                |
| query           | String | SQL query to read record from Spanner            |
| output          | String | Destination BigQuery table. format {dataset}.{table} |
| timestampBound  | String | (Optional) timestamp bound (format: yyyy-MM-ddTHH:mm:SSZ). default is strong.   |
