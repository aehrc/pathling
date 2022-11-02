---
sidebar_position: 7
---

# Kafka integration

Pathling supports Kafka as a streaming data source, and all the operations
available within the library are able to execute continuously across a stream of
data.

For more information about Spark's Kafka integration, see
the [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/3.3.1/structured-streaming-kafka-integration.html#content).

Here is an example of streaming a source of FHIR data, encoding it and then 
performing some a terminology operation upon it:

```python
from pathling import PathlingContext
from pathling.coding import Coding

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
med_administrations = pc.encode_bundle(df, "MedicationAdministration").selectExpr(
    "id", "status", "EXPLODE_OUTER(medicationCodeableConcept.coding) as coding"
)

# Perform a subsumes operation on the medication coding to determine whether it is a type of
# anti-coagulant.
result = pc.subsumes(
    med_administrations,
    "ANTICOAGULANT",
    # 372862008 |Anticoagulant|
    left_coding=Coding("http://snomed.info/sct", "372862008"),
    right_coding_column="coding",
)
```
