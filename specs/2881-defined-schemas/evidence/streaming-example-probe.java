package au.csiro.pathling.library;

import au.csiro.pathling.library.io.source.DatasetSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

/**
 * Runs the Java example of the streaming documentation (T051) against a file-based stream, for T052
 * and SC-008. The example's body is copied from the documentation, with its paths under a temporary
 * directory and two changes that make the run finish and show more than one micro-batch: the stream
 * takes one file per micro-batch, and the query runs with an available-now trigger and is awaited.
 */
class StreamingExampleProbe {

  private static final List<String> SAMPLE =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"s1\",\"gender\":\"female\","
              + "\"birthDate\":\"1970-01-01\",\"name\":[{\"family\":\"Smith\"}]}");

  private static final List<String> STREAMED =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"gender\":\"male\",\"active\":true,"
              + "\"name\":[{\"family\":\"Jones\",\"given\":[\"Tom\"]}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"birthDate\":\"1980-02-03\"}",
          "{\"resourceType\":\"Patient\",\"id\":\"p3\",\"gender\":\"other\"}");

  public static void main(final String[] args) throws Exception {
    final Path root = Files.createTempDirectory("streaming-example");
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext pc = PathlingContext.create(spark);

    // The schema set file, saved from a table of a sample, as the documentation describes.
    final Dataset<Row> sample =
        pc.encode(spark.createDataset(SAMPLE, Encoders.STRING()).toDF(), "Patient");
    final Path schemas = root.resolve("schemas.json");
    Files.writeString(schemas, SchemaSets.toJson(Map.of("Patient", sample.schema())));

    // The stream's input: one file per resource.
    final Path staging = Files.createDirectory(root.resolve("patients"));
    for (int i = 0; i < STREAMED.size(); i++) {
      Files.writeString(staging.resolve("patients-" + i + ".ndjson"), STREAMED.get(i));
    }
    final String delta = root.resolve("warehouse/delta").toString();
    final String parquet = root.resolve("warehouse/parquet").toString();

    // From here, the documentation's example.
    final StructType schema = SchemaSets.fromJson(Files.readString(schemas)).get("Patient");

    final Dataset<Row> patients =
        pc.encodeWithSchema(
            pc.getSpark().readStream().option("maxFilesPerTrigger", 1).text(staging.toString()),
            "Patient",
            PathlingContext.FHIR_JSON,
            schema);

    final StreamingQuery query =
        patients
            .writeStream()
            .option("checkpointLocation", root.resolve("checkpoints").toString())
            .trigger(Trigger.AvailableNow())
            .foreachBatch(
                (VoidFunction2<Dataset<Row>, Long>)
                    (batch, batchId) -> {
                      // Persist the micro-batch so that the two writes do not each read and
                      // encode it again.
                      batch.persist();
                      final DatasetSource data = pc.read().datasets().dataset("Patient", batch);
                      data.write().saveMode("merge").delta(delta);
                      data.write().saveMode("append").parquet(parquet);
                      batch.unpersist();
                    })
            .start();
    query.awaitTermination();

    // What the run gives.
    System.out.println("Streaming: " + patients.isStreaming());
    System.out.println("Last micro-batch: " + query.lastProgress().batchId());
    final Dataset<Row> fromDelta = pc.read().delta(delta).read("Patient");
    final Dataset<Row> fromParquet = pc.read().parquet(parquet).read("Patient");
    System.out.println("Delta schema is the schema: " + fromDelta.schema().equals(schema));
    System.out.println("Parquet schema is the schema: " + fromParquet.schema().equals(schema));
    System.out.println("Delta rows:");
    fromDelta.orderBy("id").toJSON().collectAsList().forEach(System.out::println);
    System.out.println("Parquet rows:");
    fromParquet.orderBy("id").toJSON().collectAsList().forEach(System.out::println);
    spark.stop();
  }
}
