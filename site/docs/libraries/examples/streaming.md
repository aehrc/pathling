---
sidebar_position: 3
title: Streaming queries
description: Examples of running streaming queries over FHIR data using the Pathling libraries.
---

# Streaming queries

<iframe width="560" height="315" src="https://www.youtube.com/embed/e-hFKckQITw?si=fzgfFzdB8C0X4RIX" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

Pathling supports streaming data sources, and all the operations available
within the library are able to execute continuously across a stream of data.

The following demonstrates streaming FHIR data
from [Kafka](https://kafka.apache.org/), encoding the data, and performing
terminology operations using Python:

```python
from pathling import PathlingContext, Coding, subsumes

pc = PathlingContext.create()

# Subscribe to a stream of FHIR Bundles from a Kafka topic.
df = (
    pc.spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "some-topic")
    .load()
    .selectExpr("CAST(value AS STRING) as json_bundle")
)

# Pull out the MedicationAdministration resources and put them into a dataset.
med_administrations = (
    pc.encode_bundle(df, "MedicationAdministration")
    .selectExpr(
        "id", "status",
        "EXPLODE_OUTER(medicationCodeableConcept.coding) as coding"
    )
)

# Perform a subsumes operation on the medication coding to determine whether it is a type of
# anti-coagulant.
result = med_administrations.select(
    med_administrations.id,
    med_administrations.status,
    med_administrations.coding,
    subsumes(
        # 372862008 |Anticoagulant|
        left_coding=Coding("http://snomed.info/sct", "372862008"),
        right_coding_column="coding",
    ).alias("is_anticoagulant"),
)
```

For more information about Spark's Kafka integration, see
the [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).

# Worked example: FHIR ETL pipeline with SQL on FHIR

This implementation shows an ETL (Extract, Transform, Load) pipeline that
consumes FHIR resources from Kafka, transforms them using SQL on FHIR queries
with terminology operations, and stores the results to PostgreSQL. This
example illustrates how Pathling enables streaming analytics over clinical data.

## Overview

The pipeline processes data in real time by:

1. Consumes FHIR bundles from Kafka topics
2. Extracting and encoding specific resource types (Patient, Encounter,
   Condition)
3. Transforming the data using SQL on FHIR views with [FHIRPath](/docs/fhirpath)
   expressions
4. Performing terminology operations including code translation and concept
   subsumption
5. Storing the transformed data to PostgreSQL using an upsert strategy (
   inserting new records or updating existing ones)

## Setting up the Spark environment

The initial step involves configuring Spark with the required dependencies for
FHIR
processing, Kafka connectivity, and database integration:

```python
from pathling import PathlingContext
from pyspark.sql import SparkSession
from pathling._version import __java_version__, __scala_version__
from pyspark import __version__ as __spark_version__


def _get_or_create_spark() -> SparkSession:
    """Create a Spark session configured for FHIR data processing."""
    spark_builder = SparkSession.builder.config(
        "spark.jars.packages",
        f"org.apache.spark:spark-sql-kafka-0-10_{__scala_version__}:{__spark_version__},"
        f"au.csiro.pathling:library-runtime:{__java_version__},"
        f"org.postgresql:postgresql:42.2.18",
    ).config("spark.sql.streaming.checkpointLocation", "/path/to/checkpoints")
    return spark_builder.getOrCreate()
```

This configuration incorporates:

- **Kafka connector**: For consuming streaming data
- **Pathling library**: For FHIR encoding and SQL on FHIR operations
- **PostgreSQL driver**: For database persistence
- **Checkpoint location**: For fault-tolerant streaming (enabling recovery from
  failures)

## Consuming FHIR bundles from Kafka

The pipeline subscribes to a Kafka topic containing FHIR bundles and converts
these into typed resource streams:

```python
def _subscribe_to_kafka_topic(spark: SparkSession,
                              bootstrap_servers: str,
                              topic: str) -> DataFrame:
    """Subscribe to a Kafka topic as a streaming source."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def _to_resource_stream(kafka_stream: DataFrame,
                        resource_type: str,
                        pc: PathlingContext) -> DataFrame:
    """Convert raw Kafka stream into typed FHIR resource stream."""
    from pyspark.sql.functions import explode, from_json

    json_stream = (
        kafka_stream.selectExpr("CAST(value AS STRING) AS bundle")
        .select(
            explode(
                from_json(
                    "bundle",
                    "STRUCT<entry:ARRAY<STRUCT<resource:STRING>>>",
                ).entry.resource
            ).alias("resource")
        )
        .filter(
            from_json(
                "resource",
                "STRUCT<resourceType:STRING>",
            ).resourceType == resource_type
        )
    )
    return pc.encode(json_stream, resource_type)
```

This conversion process:

1. Reads raw messages from Kafka
2. Parses FHIR bundles from JSON
3. Extracts individual resources from bundle entries
4. Filters by resource type
5. Encodes resources using Pathling for FHIRPath query support

## Defining SQL on FHIR views

The pipeline defines three views that transform raw FHIR resources into
analytical datasets. Each view uses FHIRPath expressions (a path-based query
language for FHIR) to extract and
transform clinical data.

### Patient demographics view

```python
def view_patient(data: DataSource) -> DataFrame:
    """Create a view of Patient resources with demographics."""
    return data.view(
        "Patient",
        select=[{
            "column": [
                {
                    "description": "Patient ID",
                    "path": "getResourceKey()",
                    "name": "id",
                    "type": "string",
                },
                {
                    "description": "Gender",
                    "path": "gender",
                    "name": "gender",
                    "type": "code",
                },
            ],
        }],
    )
```

### Diagnosis view with terminology operations

This view illustrates terminology processing capabilities:

```python
def view_diagnosis(data: DataSource) -> DataFrame:
    """Create a view of Condition resources with terminology operations."""
    return data.view(
        "Condition",
        select=[{
            "column": [
                {
                    "description": "Condition ID",
                    "path": "getResourceKey()",
                    "name": "id",
                },
                {
                    "description": "Patient ID",
                    "path": "subject.getReferenceKey()",
                    "name": "patient_id",
                },
                {
                    "description": "SNOMED CT diagnosis code",
                    "path": "code.coding.where(system = 'http://snomed.info/sct').code",
                    "name": "sct_id",
                },
                {
                    "description": "ICD 10-AM diagnosis code",
                    "path": "code.translate('http://aehrc.com/fhir/ConceptMap/aehrc-snomap-starter', false, 'wider').first().code",
                    "name": "icd10am_code",
                },
                {
                    "description": "Viral infection",
                    "path": "code.subsumedBy(http://snomed.info/sct|34014006 combine http://snomed.info/sct|438508001)",
                    "name": "viral_infection",
                    "type": "boolean",
                },
            ],
        }],
    )
```

Key features:

- **Code extraction**: Filters codings by system to extract SNOMED CT codes
- **Code translation**: Uses [`translate`](/docs/fhirpath#translate) to map
  SNOMED CT to ICD-10-AM via a ConceptMap
- **Concept subsumption**: Uses [`subsumedBy`](/docs/fhirpath#subsumedby) to
  detect if a condition is a viral infection by checking against parent concepts

### Encounter view with complex nested data

```python
def view_encounter(data: DataSource) -> DataFrame:
    """Create a comprehensive view of Encounter resources."""
    return data.view(
        "Encounter",
        select=[{
            "column": [
                {
                    "path": "getResourceKey()",
                    "name": "id",
                },
                {
                    "path": "subject.getReferenceKey()",
                    "name": "patient_id",
                },
                {
                    "path": "period.start",
                    "name": "start_time",
                    "type": "dateTime",
                },
            ],
            "select": [
                {
                    "forEachOrNull": "type.coding.where(system = 'http://occio.qh/data/typeofvisit')",
                    "column": [
                        {
                            "path": "code",
                            "name": "type_of_visit_code",
                        },
                        {
                            "path": "display",
                            "name": "type_of_visit_desc",
                        },
                    ],
                },
                {
                    "forEachOrNull": "priority.coding.where(system = 'http://occio.qh/data/ats')",
                    "column": [
                        {
                            "path": "code",
                            "name": "ats_code",
                        },
                    ],
                },
                {
                    "forEachOrNull": "diagnosis.where(use.coding.exists(system = 'Admission diagnosis'))",
                    "column": [
                        {
                            "path": "condition.getReferenceKey()",
                            "name": "admission_diagnosis_id",
                        },
                    ],
                },
            ],
        }],
    )
```

This view demonstrates:

- **Nested selections**: Using `forEachOrNull` to handle optional complex types
- **Filtered extractions**: Extracting specific codings based on system URLs
- **Conditional logic**: Finding admission diagnoses using FHIRPath predicates

## Connecting to a terminology server

The pipeline connects to a terminology server for real-time code validation and
translation:

```python
pc = PathlingContext.create(
    spark,
    terminology_server_url="https://terminology-service/fhir",
)
```

This enables:

- Live code validation during streaming
- Dynamic concept map translations
- Subsumption testing against terminology hierarchies

## Persisting to PostgreSQL

The pipeline implements an upsert pattern to maintain up-to-date views in
PostgreSQL:

```python
def write_postgresql(df: DataFrame, db_name: str, schema: str, view_name: str):
    """Persist DataFrame to PostgreSQL using upsert strategy."""
    import psycopg2

    columns = df.columns
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(["%s"] * len(columns))
    # Exclude 'id' from update to avoid changing primary key
    update_set = ", ".join(
        [f"{col} = EXCLUDED.{col}" for col in columns if col != "id"]
    )

    sql = f"""
    INSERT INTO {schema}.{view_name} ({insert_columns})
    VALUES ({insert_values})
    ON CONFLICT (id) DO UPDATE SET {update_set}
    """

    def upsert_partition(partition):
        conn = psycopg2.connect(
            host=host, database=db_name, user=user, password=password
        )
        cursor = conn.cursor()
        data = list(partition)
        if data:
            cursor.executemany(sql, data)
            conn.commit()
        cursor.close()
        conn.close()

    df.foreachPartition(upsert_partition)
```

This method:

- Inserts new records
- Updates existing records when IDs match
- Maintains data consistency across streaming updates
- Processes data in partitions for efficiency

## Orchestrating the complete pipeline

The main consumer function brings all components together:

```python
def start_consumer(kafka_topic: str, kafka_bootstrap_servers: str,
                   db_name: str, schema: str, host: str,
                   user: str, password: str):
    """Start the FHIR resource ETL pipeline from Kafka."""

    spark = _get_or_create_spark()
    pc = PathlingContext.create(
        spark,
        terminology_server_url="http://velonto-ontoserver-service/fhir",
    )

    # Subscribe to Kafka
    update_stream = _subscribe_to_kafka_topic()

    # Create resource streams for each type
    data = pc.read.datasets({
        resource_type: _to_resource_stream(update_stream, resource_type)
        for resource_type in ["Patient", "Encounter", "Condition"]
    })

    # Define views
    all_views = [view_patient, view_encounter, view_diagnosis]

    # Create parallel sinks
    console_sinks = []
    postgresql_sinks = []

    for view_f in all_views:
        view_name = view_f.__name__
        view_data = view_f(data)

        # Console sink for monitoring
        console_sink = (
            view_data.writeStream.outputMode("append")
            .format("console")
            .start(f"console_{view_name}")
        )
        console_sinks.append(console_sink)

        # PostgreSQL sink for persistence
        postgresql_sink = (
            view_data.writeStream.foreachBatch(
                lambda df, _, view_name=view_name: write_postgresql(
                    df, db_name, schema, view_name
                )
            )
            .outputMode("append")
            .start()
        )
        postgresql_sinks.append(postgresql_sink)

    # Block until termination
    for sink in console_sinks + postgresql_sinks:
        sink.awaitTermination()
```

## Key benefits

This architecture enables:

1. **Real-time processing**: Continuous processing of FHIR data as it arrives
2. **Complex transformations**: SQL on FHIR queries with FHIRPath expressions
3. **Terminology integration**: Live code validation and translation
4. **Fault tolerance**: Spark checkpointing enables recovery from failures
5. **Scalability**: Distributed processing across Spark clusters
6. **Data persistence**: Maintains current analytical views in PostgreSQL

## Deployment considerations

When deploying this pipeline in production:

- **Checkpointing**: Configure checkpoint directories on reliable storage (HDFS,
  S3)
- **Error handling**: Implement dead letter queues for malformed messages
- **Monitoring**: Set up metrics for lag, throughput, and error rates
- **Scaling**: Adjust Spark executor counts based on data volume
- **Schema evolution**: Plan for FHIR profile changes and version updates
- **Security**: Secure Kafka connections with SSL/SASL and database credentials

This example shows how Pathling enables real-time analytics
over FHIR data streams by combining SQL on FHIR query capabilities with
Apache Spark and Kafka scalability.
