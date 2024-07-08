---
sidebar_position: 7
description: Pathling supports Kafka as a streaming data source, and all the operations available within the library are able to execute continuously across a stream of data.
---

# Kafka integration

Pathling supports [Kafka](https://kafka.apache.org/) as a streaming data source,
and all the operations available within the library are able to execute
continuously across a stream of data.

Here is an example of streaming a source of FHIR data, encoding it and then 
performing a terminology operation upon it:

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
