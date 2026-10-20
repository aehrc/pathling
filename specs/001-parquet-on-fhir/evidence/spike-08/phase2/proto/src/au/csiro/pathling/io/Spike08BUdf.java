package au.csiro.pathling.io;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;

/**
 * Spike 08 prototype (throwaway), candidate B's JSON encode handed to Spark through a Java UDF in place
 * of {@code Encoders.row}. The UDF returns the stored row as a {@code Row} typed with the fitted schema,
 * which Spark converts with its own precompiled converters, so the generated code holds only the call and
 * stays small however wide the schema. The fit pass and the parser are B's, unchanged. The UDF is marked
 * non-deterministic so that the optimizer does not copy it into each of the top-level columns it is
 * expanded into.
 */
public final class Spike08BUdf {

  private Spike08BUdf() {}

  public static Dataset<Row> fit(Dataset<String> docs, FhirContext context, String type, String policy,
      boolean findingsColumn) {
    final Spike08Layout layout = Spike08B.fitLayout(docs, context, type, "json", findingsColumn);
    final UserDefinedFunction parse =
        udf(new Parse(layout, type, policy), layout.schema).asNondeterministic();
    final Column doc = col(docs.columns()[0]);
    return docs.select(parse.apply(doc).alias("r")).filter(col("r").isNotNull()).select("r.*");
  }

  /** One document to its stored row, or null where it is of another type or dropped. */
  static final class Parse implements UDF1<String, Row> {

    private final Spike08Layout layout;
    private final String type;
    private final String policy;
    private transient ThreadLocal<Spike08Findings> findings;

    Parse(Spike08Layout layout, String type, String policy) {
      this.layout = layout;
      this.type = type;
      this.policy = policy;
    }

    @Override
    public Row call(String doc) {
      if (findings == null) {
        findings = ThreadLocal.withInitial(
            () -> Spike08B.findings("fail".equals(policy), type, policy));
      }
      return Spike08Json.parse(doc, layout, type, findings.get());
    }
  }
}
