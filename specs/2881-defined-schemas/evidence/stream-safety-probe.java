/*
 * Probes whether each step of a read with a defined schema needs a Spark action, or fails, over a
 * streaming dataset (tasks.md T002, research R4 and R5). Run with run-probe.sh.
 *
 * Every step is built twice: over a batch dataset and over a streaming one (a text file source).
 * Building it is the step a read performs when it is called. For each, the probe reports whether it
 * failed and how many Spark jobs building it started; a job is an action that a read would run when
 * called, which a streaming dataset cannot run. A step that builds over the stream is then started
 * as a streaming query, with an available-now trigger into a memory sink, and its rows are printed.
 *
 * The schemas the steps are given are inferred, beforehand and outside the counted step, from a
 * batch read of the same text. They stand in for the schema a defined read derives, which is not
 * built yet.
 */
package au.csiro.pathling.io;

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import ca.uhn.fhir.context.FhirContext;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

class StreamSafetyProbe {

  static final String JSON_PATIENTS =
      """
      {"resourceType":"Patient","id":"p1","gender":"male","birthDate":"1970-01-01"}
      {"resourceType":"Patient","id":"p2","name":[{"family":"Smith","given":["Jo"]}]}
      """;

  static final String JSON_BUNDLES =
      """
      {"resourceType":"Bundle","type":"collection","entry":[{"fullUrl":"urn:uuid:a","resource":{"resourceType":"Patient","id":"p1","gender":"male"}},{"fullUrl":"urn:uuid:b","resource":{"resourceType":"Observation","id":"o1","status":"final","code":{"text":"x"},"subject":{"reference":"urn:uuid:a"}}}]}
      {"resourceType":"Bundle","type":"collection","entry":[{"fullUrl":"urn:uuid:c","resource":{"resourceType":"Observation","id":"o2","status":"final","code":{"text":"y"},"subject":{"reference":"Patient/p9"}}}]}
      """;

  static final String XML_PATIENTS =
      """
      <Patient xmlns="http://hl7.org/fhir"><id value="p1"/><gender value="male"/><birthDate value="1970-01-01"/></Patient>
      <Patient xmlns="http://hl7.org/fhir"><id value="p2"/><name><family value="Smith"/><given value="Jo"/></name></Patient>
      """;

  static final String XML_BUNDLES =
      """
      <Bundle xmlns="http://hl7.org/fhir"><type value="collection"/><entry><fullUrl value="urn:uuid:a"/><resource><Patient><id value="p1"/><gender value="male"/></Patient></resource></entry><entry><fullUrl value="urn:uuid:b"/><resource><Observation><id value="o1"/><status value="final"/><code><text value="x"/></code><subject><reference value="urn:uuid:a"/></subject></Observation></resource></entry></Bundle>
      <Bundle xmlns="http://hl7.org/fhir"><type value="collection"/><entry><fullUrl value="urn:uuid:c"/><resource><Observation><id value="o2"/><status value="final"/><code><text value="y"/></code><subject><reference value="Patient/p9"/></subject></Observation></resource></entry></Bundle>
      """;

  static final Map<String, String> FAIL_FAST = Map.of("mode", "FAILFAST");
  static final Map<String, String> XML_PARSE =
      Map.of("ignoreSurroundingSpaces", "false", "mode", "FAILFAST");
  static final Map<String, String> XML_INFER =
      Map.of("inferSchema", "false", "ignoreSurroundingSpaces", "false", "mode", "FAILFAST");

  static SparkSession spark;
  static final AtomicInteger JOBS = new AtomicInteger();
  static int queries = 0;

  public static void main(final String[] args) throws Exception {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    spark
        .sparkContext()
        .addSparkListener(
            new SparkListener() {
              @Override
              public void onJobStart(final SparkListenerJobStart jobStart) {
                JOBS.incrementAndGet();
              }
            });
    System.out.println("Spark " + spark.version());

    final DefinitionContext definitions = FhirDefinitionContext.of(FhirContext.forR4());
    final ResourceTransformer transformer = ResourceTransformer.of(definitions);
    final UserDefinedFunction jsonSplit = privateStatic(FhirJsonReader.class, "SPLIT");
    final UserDefinedFunction xmlSplit = privateStatic(FhirXmlReader.class, "SPLIT");
    final UserDefinedFunction xmlPrepare = privateStatic(FhirXmlReader.class, "PREPARE");

    final Path root = Files.createTempDirectory("stream-safety");
    final Input jsonPatients = input(root, "json-patients", JSON_PATIENTS);
    final Input jsonBundles = input(root, "json-bundles", JSON_BUNDLES);
    final Input xmlPatients = input(root, "xml-patients", XML_PATIENTS);
    final Input xmlBundles = input(root, "xml-bundles", XML_BUNDLES);

    // The schemas the steps are given, inferred from batch reads of the same text.
    final StructType patientJson =
        spark.read().options(FAIL_FAST).json(jsonPatients.batch).schema();
    final StructType observationJson =
        spark
            .read()
            .options(FAIL_FAST)
            .json(
                BundleResource.resources(jsonBundles.batch, jsonSplit, "Observation")
                    .select(BundleResource.DOCUMENT_COLUMN)
                    .as(Encoders.STRING()))
            .schema();
    final Dataset<String> preparedXmlPatients =
        prepare(xmlPatients.batch, xmlPrepare, "Patient").as(Encoders.STRING());
    final StructType patientXmlInferred =
        spark
            .read()
            .options(XML_INFER)
            .option("rowTag", "Patient")
            .xml(preparedXmlPatients)
            .schema();
    final XmlIngestShape patientXml =
        XmlIngestShape.of(
            DefinitionCanonicalStructure.forResource(definitions, "Patient"),
            "Patient",
            patientXmlInferred);
    final StructType observationXmlInferred =
        spark
            .read()
            .options(XML_INFER)
            .option("rowTag", "Observation")
            .xml(
                BundleResource.resources(xmlBundles.batch, xmlSplit, "Observation")
                    .select(BundleResource.DOCUMENT_COLUMN)
                    .as(Encoders.STRING()))
            .schema();
    final XmlIngestShape observationXml =
        XmlIngestShape.of(
            DefinitionCanonicalStructure.forResource(definitions, "Observation"),
            "Observation",
            observationXmlInferred);

    System.out.println("\n== Steps named by T002");
    probe(
        "spark.read().schema(s).json(Dataset<String>), JSON documents",
        jsonPatients,
        docs -> spark.read().schema(patientJson).options(FAIL_FAST).json(docs).toDF());
    probe(
        "from_json, JSON documents",
        jsonPatients,
        docs ->
            docs.select(
                functions.from_json(functions.col("value"), patientJson, FAIL_FAST).alias("p")));
    probe(
        "FhirJsonReader SPLIT + explode (BundleResource.resources), JSON bundles",
        jsonBundles,
        docs -> BundleResource.resources(docs, jsonSplit, "Observation"));
    probe(
        "FhirXmlReader SPLIT + explode (BundleResource.resources), XML bundles",
        xmlBundles,
        docs -> BundleResource.resources(docs, xmlSplit, "Observation"));
    probe(
        "from_xml, XML documents (after PREPARE)",
        xmlPatients,
        docs ->
            prepare(docs, xmlPrepare, "Patient")
                .select(
                    functions
                        .from_xml(
                            functions.col(BundleResource.DOCUMENT_COLUMN),
                            patientXml.parseSchema(),
                            XML_PARSE)
                        .alias("p")));

    System.out.println("\n== Further steps a defined read keeps, beyond T002's list");
    probe(
        "FhirXmlReader PREPARE + explode, XML documents",
        xmlPatients,
        docs -> prepare(docs, xmlPrepare, "Patient"));
    probe(
        "JSON documents end to end: from_json, then toLayout",
        jsonPatients,
        docs ->
            docs.select(
                    functions
                        .from_json(functions.col("value"), patientJson, FAIL_FAST)
                        .alias(BundleResource.PARSED))
                .select(
                    transformer.toLayout(
                        "Patient",
                        patientJson,
                        functions.col(BundleResource.PARSED),
                        UnaryOperator.identity())));
    probe(
        "JSON bundles end to end: split, from_json, toLayout resolving references",
        jsonBundles,
        docs -> {
          final Dataset<Row> resources = BundleResource.resources(docs, jsonSplit, "Observation");
          final Column references = functions.col(BundleResource.REFERENCES_COLUMN);
          return resources
              .withColumn(
                  BundleResource.PARSED,
                  functions.from_json(
                      functions.col(BundleResource.DOCUMENT_COLUMN), observationJson, FAIL_FAST))
              .select(
                  transformer.toLayout(
                      "Observation",
                      observationJson,
                      functions.col(BundleResource.PARSED),
                      reference -> BundleResource.resolve(reference, references)));
        });
    probe(
        "XML documents end to end: PREPARE, from_xml, shape, toLayout",
        xmlPatients,
        docs ->
            xmlToLayout(
                transformer, patientXml, "Patient", prepare(docs, xmlPrepare, "Patient"), null));
    probe(
        "XML bundles end to end: split, from_xml, shape, toLayout resolving references",
        xmlBundles,
        docs ->
            xmlToLayout(
                transformer,
                observationXml,
                "Observation",
                BundleResource.resources(docs, xmlSplit, "Observation"),
                functions.col(BundleResource.REFERENCES_COLUMN)));

    System.out.println("\n== For contrast: the inference read of today's reads, over a batch");
    probe(
        "spark.read().json(Dataset<String>) without a schema, JSON documents",
        jsonPatients,
        docs -> spark.read().options(FAIL_FAST).json(docs).toDF());
    probe(
        "spark.read().xml(Dataset<String>) without a schema, XML documents",
        xmlPatients,
        docs ->
            spark
                .read()
                .options(XML_INFER)
                .option("rowTag", "Patient")
                .xml(prepare(docs, xmlPrepare, "Patient").as(Encoders.STRING()))
                .toDF());

    spark.stop();
  }

  /** The private steps of FhirXmlReader.toLayout after its inference read, given its plan. */
  static Dataset<Row> xmlToLayout(
      final ResourceTransformer transformer,
      final XmlIngestShape shape,
      final String resourceType,
      final Dataset<Row> documents,
      final Column references) {
    final Column document = functions.col(BundleResource.DOCUMENT_COLUMN);
    final Dataset<Row> parsed =
        documents.withColumn(
            BundleResource.PARSED, functions.from_xml(document, shape.parseSchema(), XML_PARSE));
    final Column shaped = shape.shape(functions.col(BundleResource.PARSED), document);
    final Dataset<Row> shapedRows =
        references == null
            ? parsed.select(shaped.alias("shaped"))
            : parsed.select(shaped.alias("shaped"), references);
    final StructType observed = (StructType) shapedRows.schema().apply("shaped").dataType();
    final UnaryOperator<Column> resolution =
        references == null
            ? UnaryOperator.identity()
            : reference -> BundleResource.resolve(reference, references);
    return shapedRows.select(
        transformer.toLayout(resourceType, observed, functions.col("shaped"), resolution));
  }

  static Dataset<Row> prepare(
      final Dataset<String> documents, final UserDefinedFunction prepare, final String type) {
    return documents.select(
        functions
            .explode(prepare.apply(functions.col(documents.columns()[0]), functions.lit(type)))
            .alias(BundleResource.DOCUMENT_COLUMN));
  }

  record Input(Dataset<String> batch, Dataset<String> stream) {}

  static Input input(final Path root, final String name, final String text) throws Exception {
    final Path dir = Files.createDirectories(root.resolve(name));
    Files.writeString(dir.resolve("part-0.txt"), text);
    return new Input(
        spark.read().text(dir.toString()).as(Encoders.STRING()),
        spark.readStream().text(dir.toString()).as(Encoders.STRING()));
  }

  static void probe(
      final String label, final Input input, final Function<Dataset<String>, Dataset<Row>> step)
      throws Exception {
    System.out.println("\n--- " + label);
    build("batch", input.batch, step);
    final Dataset<Row> streamed = build("stream", input.stream, step);
    if (streamed == null) {
      return;
    }
    final String name = "q" + queries++;
    try {
      final StreamingQuery query =
          streamed
              .writeStream()
              .format("memory")
              .queryName(name)
              .trigger(Trigger.AvailableNow())
              .start();
      query.awaitTermination();
      final List<Row> rows = spark.table(name).collectAsList();
      System.out.println("    stream query ran: " + rows.size() + " rows");
      rows.forEach(row -> System.out.println("      " + clip(row.toString())));
    } catch (final Exception e) {
      System.out.println("    stream query FAILED: " + firstLine(e));
    }
  }

  /** Builds a step over a dataset, reporting any failure and the jobs that building it started. */
  static Dataset<Row> build(
      final String kind,
      final Dataset<String> input,
      final Function<Dataset<String>, Dataset<Row>> step)
      throws Exception {
    spark.sparkContext().listenerBus().waitUntilEmpty();
    final int before = JOBS.get();
    Dataset<Row> result = null;
    String outcome;
    try {
      result = step.apply(input);
      outcome = "built, schema " + clip(result.schema().simpleString());
    } catch (final Exception e) {
      outcome = "FAILED: " + firstLine(e);
    }
    spark.sparkContext().listenerBus().waitUntilEmpty();
    System.out.println(
        "    " + kind + ": jobs started while building: " + (JOBS.get() - before) + "; " + outcome);
    return result;
  }

  static String firstLine(final Throwable e) {
    final String message = String.valueOf(e.getMessage()).strip().lines().findFirst().orElse("");
    return e.getClass().getSimpleName()
        + ": "
        + message.substring(0, Math.min(message.length(), 300));
  }

  static String clip(final String text) {
    return text.length() <= 200 ? text : text.substring(0, 200) + "...";
  }

  @SuppressWarnings("unchecked")
  static <T> T privateStatic(final Class<?> type, final String name) throws Exception {
    final Field field = type.getDeclaredField(name);
    field.setAccessible(true);
    return (T) field.get(null);
  }
}
