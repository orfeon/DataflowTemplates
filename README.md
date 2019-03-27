# DataflowTemplates
Useful Cloud Dataflow custom templates.

You should use [Google provided templates](https://github.com/GoogleCloudPlatform/DataflowTemplates) if your use case fits.
This templates target use cases that official templates do not cover.

## Template Pipelines

* [Spanner to GCS Text](src/main/java/net/orfeon/cloud/dataflow/templates/SpannerToText.java)
* [Spanner to GCS Avro](src/main/java/net/orfeon/cloud/dataflow/templates/SpannerToAvro.java)
* [Spanner to BigQuery](src/main/java/net/orfeon/cloud/dataflow/templates/SpannerToBigQuery.java)
* [Spanner to Spanner](src/main/java/net/orfeon/cloud/dataflow/templates/SpannerToSpanner.java)
* [Spanner Bulk Delete](src/main/java/net/orfeon/cloud/dataflow/templates/SpannerToSpannerDelete.java)
* [BigQuery to Spanner](src/main/java/net/orfeon/cloud/dataflow/templates/BigQueryToSpanner.java)
* [BigQuery to Datastore](src/main/java/net/orfeon/cloud/dataflow/templates/BigQueryToDatastore.java)
* [GCS Avro to Spanner](src/main/java/net/orfeon/cloud/dataflow/templates/AvroToSpanner.java)
* [GCS Avro to Datastore](src/main/java/net/orfeon/cloud/dataflow/templates/AvroToDatastore.java)
* [JDBC to GCS Avro](src/main/java/net/orfeon/cloud/dataflow/templates/JdbcToAvro.java)
* [Dummy to Spanner](src/main/java/net/orfeon/cloud/dataflow/templates/DummyToSpanner.java)
* [Spanner to BigQueryML to Spanner](src/main/java/net/orfeon/cloud/dataflow/templates/ml/bigquery/SpannerToBQMLToSpanner.java)

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
SpannerToText support json and csv format.

| Parameter       | Type   | Description                                      |
|-----------------|--------|--------------------------------------------------|
| type            | String | Output format. set `json`(default) or `csv`.     |
| projectId       | String | projectID for Spanner you will read.             |
| instanceId      | String | Spanner instanceID you will read.                |
| databaseId      | String | Spanner databaseID you will read.                |
| query           | String | SQL query to read record from Spanner            |
| output          | String | GCS path to output. prefix must start with gs:// |
| timestampBound  | String | (Optional) timestamp bound (format: yyyy-MM-ddTHH:mm:SSZ). default is strong.   |

* If query is root partitionable(query plan have a DistributedUnion at the root), Pipeline will read record from Spanner in parallel.
For example, query that includes 'order by', 'limit' operation can not have DistributedUnion at the root.
Please run EXPLAIN for query plan details before running template.
* [timestampBound](https://cloud.google.com/spanner/docs/timestamp-bounds) must be within one hour.
* timestampBound format example in japan: '2018-10-01T18:00:00+09:00'.
* Query will be split and executed in parallel if the delimiter string `--SPLITTER--` present.


### SpannerToAvro

SpannerToAvro's feature is that user can specify sql to extract record as template parameter.
You can restore Spanner table, BigQuery table from avro files you outputted.
Template parameters are same as SpannerToText.

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
* Query will be split and executed in parallel if the delimiter string `--SPLITTER--` present.


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

* Query will be split and executed in parallel if the delimiter string `--SPLITTER--` present.


### SpannerToSpanner

SpannerToSpanner enables you to query from some Spanner table and write results to specified Spanner table using template.
Template parameters are same as SpannerToText.
Spanner destination table must be created.

| Parameter       | Type   | Description                                        |
|-----------------|--------|----------------------------------------------------|
| inputProjectId  | String | projectID for Spanner you will read.               |
| inputInstanceId | String | Spanner instanceID you will read.                  |
| inputDatabaseId | String | Spanner databaseID you will read.                  |
| query           | String | SQL query to read record from Spanner              |
| outputProjectId | String | projectID for Spanner you will write query result. |
| outputInstanceId| String | Spanner instanceID you will write query result.    |
| outputDatabaseId| String | Spanner databaseID you will write query result.    |
| table           | String | Spanner table name you will write query result.    |
| mutationOp      | String | Spanner [insert policy](https://googleapis.github.io/google-cloud-java/google-cloud-clients/apidocs/com/google/cloud/spanner/Mutation.Op.html). `INSERT` or `UPDATE` or `REPLACE` or `INSERT_OR_UPDATE` |
| outputError     | String | GCS path to output error record as avro files.     |
| timestampBound  | String | (Optional) timestamp bound (format: yyyy-MM-ddTHH:mm:SSZ). default is strong.   |

* Query will be split and executed in parallel if the delimiter string `--SPLITTER--` present.


### SpannerToSpannerDelete

SpannerToSpannerDelete delete records from table that matched query results user specified.

| Parameter       | Type   | Description                                      |
|-----------------|--------|--------------------------------------------------|
| projectId       | String | projectID for Spanner you will read.             |
| instanceId      | String | Spanner instanceID you will read.                |
| databaseId      | String | Spanner databaseID you will read.                |
| query           | String | SQL query to read record from Spanner            |
| table           | String | Spanner table name to delete records             |
| keyFields       | String | Key fields in query results. If composite key case, set comma-separated fields in key sequence. |
| timestampBound  | String | (Optional) timestamp bound (format: yyyy-MM-ddTHH:mm:SSZ). default is strong.   |

* Query will be split and executed in parallel if the delimiter string `--SPLITTER--` present.


### AvroToSpanner

AvroToSpanner recovers Spanner table from avro files you made using SpannerToAvro template.
Template parameters are same as SpannerToText.

| Parameter       | Type   | Description                                      |
|-----------------|--------|--------------------------------------------------|
| input           | String | GCS path for avro files. prefix must start with gs:// |
| projectId       | String | projectID for Spanner you will recover.          |
| instanceId      | String | Spanner instanceID you will recover.             |
| databaseId      | String | Spanner databaseID you will recover.             |
| table           | String | Spanner table name to insert records.            |
| mutationOp      | String | Spanner [insert policy](https://googleapis.github.io/google-cloud-java/google-cloud-clients/apidocs/com/google/cloud/spanner/Mutation.Op.html). `INSERT` or `UPDATE` or `REPLACE` or `INSERT_OR_UPDATE` |


### AvroToDatastore

AvroToDatastore inserts avro files on GCS to Cloud Datastore.

| Parameter              | Type   | Description                                      |
|------------------------|--------|--------------------------------------------------|
| input                  | String | GCS path for avro files. prefix must start with gs:// |
| projectId              | String | GCP Project ID for Datastore to insert.          |
| kind                   | String | Datastore kind name to insert.                   |
| keyField               | String | Unique key field name in avro record.            |
| excludeFromIndexFields | String | (Optional) Field names to exclude from index.    |


### BigQueryToSpanner

BigQueryToSpanner enables you to query from BigQuery and write results to specified Spanner table using template.
Spanner destination table will be created if not exists.

| Parameter       | Type   | Description                                        |
|-----------------|--------|----------------------------------------------------|
| query           | String | SQL query to read record from BigQuery             |
| projectId       | String | projectID for Spanner you will write query result. |
| instanceId      | String | Spanner instanceID you will write query result.    |
| databaseId      | String | Spanner databaseID you will write query result.    |
| table           | String | Spanner table name you will write query result.    |
| mutationOp      | String | Spanner [insert policy](https://googleapis.github.io/google-cloud-java/google-cloud-clients/apidocs/com/google/cloud/spanner/Mutation.Op.html). `INSERT` or `UPDATE` or `REPLACE` or `INSERT_OR_UPDATE` |
| outputError     | String | GCS path to output error record as avro files.     |
| primaryKeyFields| String | (Optional) Key field on destination Spanner table. (Required if use table auto generation) |

* You must enable [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage/).
* At this time, Worker requires high memory to use Storage API. If the process does not work, please increase the worker size.


### BigQueryToDatastore

BigQueryToDatastore enables you to query from BigQuery and write results to specified Cloud Datastore kind using template.

| Parameter              | Type   | Description                                          |
|------------------------|--------|------------------------------------------------------|
| query                  | String | SQL query to read record from BigQuery               |
| projectId              | String | projectID for Datastore you will write query result. |
| kind                   | String | Cloud Datastore target kind name to store.           |
| keyField               | String | Unique field name in query results from BigQuery.    |
| excludeFromIndexFields | String | (Optional) Field names to exclude from index.        |

* You must enable [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage/).
* At this time, Worker requires high memory to use Storage API. If the process does not work, please increase the worker size.


### JdbcToAvro

JdbcToAvro can read data from Jdbc using free SQL and save it in Avro format.

| Parameter   | Type   | Description                                      |
|-------------|--------|--------------------------------------------------|
| query       | String | SQL query to read record from Database           |
| driverClass | String | `com.mysql.cj.jdbc.Driver` or `org.postgresql.Driver`.|
| url         | String | DB connection url.|
| username    | String | Database username to access. |
| password    | String | Database access user's password. |
| output      | String | GCS path to output. prefix must start with gs:// |
| cyptoKeyName| String | (Optional)Cloud KMS cyptoKeyName to decrypt password |

* Query will be split and executed in parallel if the delimiter string `--SPLITTER--` present.
* `url` format example (Cloud SQL): jdbc:mysql://google/{database}?cloudSqlInstance={project}:{region}:{instance}&socketFactory=com.google.cloud.sql.mysql.SocketFactory
* cyptoKeyName format: projects/{project}/locations/{location}/keyRings/{keyRings}/cryptoKeys/{cryptoKey}
* When use cyptoKeyName, encrypt password and apply base64.
* SQL data type will be converted as [this code](src/main/java/net/orfeon/cloud/dataflow/spanner/StructUtil.java#L107)


### DummyToSpanner

DummyToSpanner insert dummy records to specified Spanner table.

| Parameter   | Type   | Description                                      |
|-------------|--------|--------------------------------------------------|
| projectId   | String | Spanner projectID.  |
| instanceId  | String | Spanner instanceID. |
| databaseId  | String | Spanner databaseID. |
| tables      | String | Spanner table names and count you want to insert dummy records. format: tableName1:1000,tableName2:20000   |
| config      | String | (Optional) Config yaml file. GCS path or yaml text itself.  |
| parallelNum | Integer | (Optional) Worker parallel num to generate dummy records.  |

* You can specify range of random values each fields.
* The format of the config yaml file format is as follows.

```yaml
tables:
  - name: dummyTable1
    randomRate: 0
    fields:
      - name: intField
        range: [1,10]
      - name: floatField
        range: [-100.0,100.0]
      - name: dateField
        range: [2014-01-01,2018-01-01]
      - name: timestampField
        range: [2018-01-01T00:00:00,2019-01-01T00:00:00]
```

* `randomRate` describes null value rate for `NULLABLE` field.
* `range` describes minimum and maximum value for the field.


### SpannerToBQMLToSpanner

SpannerToBQMLToSpanner can apply BigQueryML prediction to Spanner query result records.
You have to create spanner table that store prediction results before run this template.
Prediction results table schema has only two columns, {recordKey} and score.
{recordKey} is unique key field name in your spanner query results.
score is prediction result. Data type must be FLOAT64.
In Linear regression, score represents target value prediction.
In Logistic regression, score represents probability the label later one(order by label name) will occur.

| Parameter       | Type   | Description                                        |
|-----------------|--------|----------------------------------------------------|
| model           | String | BigQuery ML model. Format: {dataset}.{model_name}  |
| modelType       | String | BigQuery ML model type. `linear_reg` or `logistic_reg` |
| keyFieldName    | String | Unique key field name in spanner query result records. |
| inputProjectId  | String | projectID for Spanner you will read.               |
| inputInstanceId | String | Spanner instanceID you will read.                  |
| inputDatabaseId | String | Spanner databaseID you will read.                  |
| query           | String | SQL query to read record from Spanner              |
| outputProjectId | String | projectID for Spanner you will write query result. |
| outputInstanceId| String | Spanner instanceID you will write prediction result. |
| outputDatabaseId| String | Spanner databaseID you will write prediction result. |
| table           | String | Spanner table name you will write prediction result. |
| mutationOp      | String | Spanner [insert policy](https://googleapis.github.io/google-cloud-java/google-cloud-clients/apidocs/com/google/cloud/spanner/Mutation.Op.html). `INSERT` or `UPDATE` or `REPLACE` or `INSERT_OR_UPDATE` |
| outputError     | String | GCS path to output error record as avro files.     |
| timestampBound  | String | (Optional) timestamp bound (format: yyyy-MM-ddTHH:mm:SSZ). default is strong.   |

* Query will be split and executed in parallel if the delimiter string `--SPLITTER--` present.
* In Logistic regression, when labels contains `A` and `B`, score represents probability B occur.
