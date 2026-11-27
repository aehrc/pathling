import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJson {

  private static final String PATIENT =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"given\":[\"Ann\",null,\"Bee\"],"
          + "\"_given\":[null,{\"extension\":[{\"url\":\"http://example.com/dar\","
          + "\"valueCode\":\"masked\"}]},null]},{\"family\":\"Only\"}]}";

  // A name carrying nothing the definitions describe, next to one that does.
  private static final String UNDESCRIBED =
      "{\"resourceType\":\"Patient\",\"id\":\"2\","
          + "\"name\":[{\"bogusChild\":\"y\"},{\"family\":\"Smith\"}]}";

  public static void main(final String[] args) throws Exception {
    final SparkSession spark =
        SparkSession.builder().appName("sparkjson").master("local[2]").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    for (final String document :
        List.of(
            PATIENT,
            UNDESCRIBED,
            "{\"resourceType\":\"Patient\",\"id\":\"3\",\"name\":[{\"family\":null},{\"family\":\"S\"}]}")) {
      final Dataset<String> input = spark.createDataset(List.of(document), Encoders.STRING());
      final Dataset<Row> read = spark.read().json(input);
      System.out.println("--- schema");
      read.printSchema();
      for (final String expression : List.of("name[0].given", "name[0]._given", "name")) {
        try {
          System.out.println(
              "--- " + expression + ": " + read.selectExpr(expression).collectAsList());
        } catch (final Exception e) {
          System.out.println("--- " + expression + ": unavailable");
        }
      }
      final Path out = Files.createTempDirectory("sparkjson");
      Files.delete(out);
      read.repartition(1).write().json(out.toString());
      try (final Stream<Path> files = Files.list(out)) {
        files
            .filter(p -> p.toString().endsWith(".json"))
            .forEach(
                p -> {
                  try {
                    System.out.println("--- written: " + Files.readString(p).trim());
                  } catch (final Exception e) {
                    throw new RuntimeException(e);
                  }
                });
      }
    }
    spark.stop();
  }
}
