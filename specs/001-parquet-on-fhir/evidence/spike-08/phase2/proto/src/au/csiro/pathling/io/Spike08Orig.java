package au.csiro.pathling.io;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;

import au.csiro.pathling.spike08.orig.FhirJsonReader;
import au.csiro.pathling.spike08.orig.FhirJsonWriter;
import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Spike 08 prototype (throwaway), the entry points of the original Parquet on FHIR spike (branch
 * {@code spike/parquet-on-fhir}, linked from #2367), whose reader and writer are ported in {@code
 * au.csiro.pathling.spike08.orig} with only the changes Spark 4 forces. It is timed for reference, as
 * a floor of the Spark-inference approach: it makes no checks and does not build the stored layout.
 * The fit is the reader's {@code spark.read().json} inference and its transform; decode applies the
 * writer's transform and renders each row as {@code write().json} would, with {@code to_json}.
 */
public final class Spike08Orig {

  private Spike08Orig() {}

  public static Dataset<Row> fit(final Dataset<String> docs, final String type) {
    return new FhirJsonReader(type, "R4", Map.of()).read(docs);
  }

  public static Dataset<Row> decode(final Dataset<Row> stored, final String type) {
    final Dataset<Row> out = new FhirJsonWriter("R4", type).transform(stored);
    final Column[] cols = Arrays.stream(out.columns()).map(c -> col("`" + c + "`")).toArray(Column[]::new);
    return out.select(to_json(struct(cols)).alias("json"));
  }
}
