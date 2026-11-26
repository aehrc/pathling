# Whether each step of a defined read needs an action, or fails, over a streaming dataset

Measured on 2026-11-26 with Spark 4.1.0, the version the build uses, against the `io` module at
commit `43c0e6147e`, using [stream-safety-probe.java](stream-safety-probe.java), run with
`./run-probe.sh stream-safety-probe.java StreamSafetyProbe` after building `io` (see
[run-probe.sh](run-probe.sh)). It was run for T002 and research R4 and R5.

Each step is built twice from the same text: over a batch dataset and over a streaming one, a
`readStream().text` file source. Building a step is what a read does when it is called. The probe
counts the Spark jobs started while building it, with a listener and the listener bus drained before
and after, since a job started at that point is an action a read would run when called, which a
stream cannot run. A step that builds over the stream is then started as a streaming query, with an
available-now trigger into a memory sink, and its rows are printed. The schemas the steps are given
are inferred beforehand, outside the counted step, from batch reads of the same text, standing in for
the schema a defined read will derive. The bundle splitters and `PREPARE` are reached by reflection,
being private constants of the readers.

**Result: of the four steps T002 names, only `spark.read().json(Dataset<String>)` with a given
schema fails over a stream. It fails when built, with `Queries with streaming sources must be
executed with writeStream.start()`, and starts no job; over a batch it also starts no job. `from_json`,
the JSON and XML bundle splitters followed by `explode`, and `from_xml` start no job, build over a
stream, and run as a streaming query with the expected rows.**

| Step                                                                     | Jobs when built (batch, stream) | Over a stream              |
| ------------------------------------------------------------------------ | ------------------------------- | -------------------------- |
| `spark.read().schema(s).json(Dataset<String>)`                           | 0, 0                            | fails when built           |
| `from_json`                                                              | 0, 0                            | runs, 2 rows               |
| `BundleResource.resources` with `FhirJsonReader`'s `SPLIT`, then explode | 0, 0                            | runs, 2 rows, URN resolved |
| `BundleResource.resources` with `FhirXmlReader`'s `SPLIT`, then explode  | 0, 0                            | runs, 2 rows, URN resolved |
| `from_xml` (after `PREPARE`)                                             | 0, 0                            | runs, 2 rows               |

Beyond T002's list, the probe also built the rest of each defined read over a stream, with the
given schema in place of the inference read and nothing else changed:

| Step                                                                                    | Jobs when built (batch, stream) | Over a stream                             |
| --------------------------------------------------------------------------------------- | ------------------------------- | ----------------------------------------- |
| `FhirXmlReader`'s `PREPARE`, then explode                                               | 0, 0                            | runs, 2 rows                              |
| JSON documents: `from_json`, then `toLayout`                                            | 0, 0                            | runs, 2 rows                              |
| JSON bundles: split, `from_json`, `toLayout` resolving references                       | 0, 0                            | runs; `urn:uuid:a` stored as `Patient/p1` |
| XML documents: `PREPARE`, `from_xml`, `XmlIngestShape.shape`, `toLayout`                | 0, 0                            | runs, 2 rows                              |
| XML bundles: split, `from_xml`, `XmlIngestShape.shape`, `toLayout` resolving references | 0, 0                            | runs; `urn:uuid:a` stored as `Patient/p1` |

For contrast, today's inference reads, `spark.read().json(Dataset<String>)` and
`spark.read().xml(Dataset<String>)` without a schema, each start one job when built over a batch,
and fail when built over a stream with the same error.

## Consequences

- R4 holds: `spark.read().json(Dataset<String>)` is not stream-safe even with a schema, so a defined
  read of a dataset of JSON documents uses `from_json`. It fails at analysis rather than by running
  an action: with a schema given it starts no job, over a batch or a stream.
- R4's bundle steps and R5's XML steps hold: with the inference read replaced by a given schema,
  every remaining step of the JSON and XML document and bundle reads builds with no job and runs over
  a stream, references between a bundle's entries included.
- Not measured: file reads (`read(type, path)`), which take a path rather than a dataset and so are
  not streaming; the XML parse schema that a defined read derives with `XmlIngestShape` from the
  defined schema, since that derivation does not exist yet (here it came from inference, so a
  repeating element that occurs once is parsed as a struct, which `XmlIngestShape` accepts); and
  sources other than a file stream.

## Output

```text
Spark 4.1.0

== Steps named by T002

--- spark.read().schema(s).json(Dataset<String>), JSON documents
    batch: jobs started while building: 0; built, schema struct<birthDate:string,gender:string,id:string,name:array<struct<family:string,given:array<string>>>,resourceType:string>
    stream: jobs started while building: 0; FAILED: ExtendedAnalysisException: Queries with streaming sources must be executed with writeStream.start(), or from a streaming table or flow definition within a Spark Declarative Pipeline.;

--- from_json, JSON documents
    batch: jobs started while building: 0; built, schema struct<p:struct<birthDate:string,gender:string,id:string,name:array<struct<family:string,given:array<string>>>,resourceType:string>>
    stream: jobs started while building: 0; built, schema struct<p:struct<birthDate:string,gender:string,id:string,name:array<struct<family:string,given:array<string>>>,resourceType:string>>
    stream query ran: 2 rows
      [[1970-01-01,male,p1,null,Patient]]
      [[null,null,p2,ArraySeq([Smith,ArraySeq(Jo)]),Patient]]

--- FhirJsonReader SPLIT + explode (BundleResource.resources), JSON bundles
    batch: jobs started while building: 0; built, schema struct<document:string,references:map<string,string>>
    stream: jobs started while building: 0; built, schema struct<document:string,references:map<string,string>>
    stream query ran: 2 rows
      [{"resourceType":"Observation","id":"o1","status":"final","code":{"text":"x"},"subject":{"reference":"urn:uuid:a"}},Map(urn:uuid:a -> Patient/p1)]
      [{"resourceType":"Observation","id":"o2","status":"final","code":{"text":"y"},"subject":{"reference":"Patient/p9"}},Map()]

--- FhirXmlReader SPLIT + explode (BundleResource.resources), XML bundles
    batch: jobs started while building: 0; built, schema struct<document:string,references:map<string,string>>
    stream: jobs started while building: 0; built, schema struct<document:string,references:map<string,string>>
    stream query ran: 2 rows
      [<Observation xmlns="http://hl7.org/fhir"><id value="o1"/><status value="final"/><code><text value="x"/></code><subject><reference value="urn:uuid:a"/></subject></Observation>,Map(urn:uuid:a -> Patien...
      [<Observation xmlns="http://hl7.org/fhir"><id value="o2"/><status value="final"/><code><text value="y"/></code><subject><reference value="Patient/p9"/></subject></Observation>,Map()]

--- from_xml, XML documents (after PREPARE)
    batch: jobs started while building: 0; built, schema struct<p:struct<birthDate:struct<_value:string>,gender:struct<_value:string>,id:struct<_value:string>,name:struct<family:struct<_value:string>,given:struct<_value:string>>>>
    stream: jobs started while building: 0; built, schema struct<p:struct<birthDate:struct<_value:string>,gender:struct<_value:string>,id:struct<_value:string>,name:struct<family:struct<_value:string>,given:struct<_value:string>>>>
    stream query ran: 2 rows
      [[[1970-01-01],[male],[p1],null]]
      [[null,null,[p2],[[Smith],[Jo]]]]

== Further steps a defined read keeps, beyond T002's list

--- FhirXmlReader PREPARE + explode, XML documents
    batch: jobs started while building: 0; built, schema struct<document:string>
    stream: jobs started while building: 0; built, schema struct<document:string>
    stream query ran: 2 rows
      [<Patient xmlns="http://hl7.org/fhir"><id value="p1"/><gender value="male"/><birthDate value="1970-01-01"/></Patient>]
      [<Patient xmlns="http://hl7.org/fhir"><id value="p2"/><name><family value="Smith"/><given value="Jo"/></name></Patient>]

--- JSON documents end to end: from_json, then toLayout
    batch: jobs started while building: 0; built, schema struct<resourceType:string,id:string,name:array<struct<family:string,given:array<string>>>,gender:string,birthDate:string>
    stream: jobs started while building: 0; built, schema struct<resourceType:string,id:string,name:array<struct<family:string,given:array<string>>>,gender:string,birthDate:string>
    stream query ran: 2 rows
      [Patient,p1,null,male,1970-01-01]
      [Patient,p2,ArraySeq([Smith,ArraySeq(Jo)]),null,null]

--- JSON bundles end to end: split, from_json, toLayout resolving references
    batch: jobs started while building: 0; built, schema struct<resourceType:string,id:string,status:string,code:struct<text:string>,subject:struct<reference:string>>
    stream: jobs started while building: 0; built, schema struct<resourceType:string,id:string,status:string,code:struct<text:string>,subject:struct<reference:string>>
    stream query ran: 2 rows
      [Observation,o1,final,[x],[Patient/p1]]
      [Observation,o2,final,[y],[Patient/p9]]

--- XML documents end to end: PREPARE, from_xml, shape, toLayout
    batch: jobs started while building: 0; built, schema struct<resourceType:string,id:string,name:array<struct<family:string,given:array<string>>>,gender:string,birthDate:string>
    stream: jobs started while building: 0; built, schema struct<resourceType:string,id:string,name:array<struct<family:string,given:array<string>>>,gender:string,birthDate:string>
    stream query ran: 2 rows
      [Patient,p1,null,male,1970-01-01]
      [Patient,p2,ArraySeq([Smith,ArraySeq(Jo)]),null,null]

--- XML bundles end to end: split, from_xml, shape, toLayout resolving references
    batch: jobs started while building: 0; built, schema struct<resourceType:string,id:string,status:string,code:struct<text:string>,subject:struct<reference:string>>
    stream: jobs started while building: 0; built, schema struct<resourceType:string,id:string,status:string,code:struct<text:string>,subject:struct<reference:string>>
    stream query ran: 2 rows
      [Observation,o1,final,[x],[Patient/p1]]
      [Observation,o2,final,[y],[Patient/p9]]

== For contrast: the inference read of today's reads, over a batch

--- spark.read().json(Dataset<String>) without a schema, JSON documents
    batch: jobs started while building: 1; built, schema struct<birthDate:string,gender:string,id:string,name:array<struct<family:string,given:array<string>>>,resourceType:string>
    stream: jobs started while building: 0; FAILED: ExtendedAnalysisException: Queries with streaming sources must be executed with writeStream.start(), or from a streaming table or flow definition within a Spark Declarative Pipeline.;

--- spark.read().xml(Dataset<String>) without a schema, XML documents
    batch: jobs started while building: 1; built, schema struct<_xmlns:string,birthDate:struct<_value:string>,gender:struct<_value:string>,id:struct<_value:string>,name:struct<family:struct<_value:string>,given:struct<_value:string>>>
    stream: jobs started while building: 0; FAILED: ExtendedAnalysisException: Queries with streaming sources must be executed with writeStream.start(), or from a streaming table or flow definition within a Spark Declarative Pipeline.;
```
