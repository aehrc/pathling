/**
 * Adds element-level metadata to encoded Patients and Encounters, in the Parquet on FHIR shape
 * (`_x` groups beside `x`, extensions on elements). Both variants get the same schema: with ext
 * false every metadata field is present and null, so that ext against plain differs in content only.
 * Payloads are five string extensions of 128 to 199 hex characters, unique per element.
 */
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class Splice {

  static final String X = "array<struct<url:string,valueString:string>>";

  /** One payload string, 128 to 199 characters, from a seed expression. */
  static String payload(final String seed, final int k) {
    final String s = "concat(" + seed + ", '#" + k + "')";
    return "concat(sha2(" + s + ", 512), substr(sha2(" + s + ", 256), 1, pmod(hash(" + s + "), 72)))";
  }

  /** Five extensions, or a typed null in the plain variant. */
  static String exts(final String seed, final boolean ext) {
    if (!ext) {
      return "cast(null as " + X + ")";
    }
    final StringBuilder b = new StringBuilder("array(");
    for (int k = 0; k < 5; k++) {
      b.append(k > 0 ? ", " : "")
          .append("named_struct('url', 'http://example.org/ext/")
          .append(k)
          .append("', 'valueString', ")
          .append(payload(seed, k))
          .append(")");
    }
    return b.append(")").toString();
  }

  public static Dataset<Row> patient(final Dataset<Row> df, final boolean ext) {
    final String given =
        ext
            ? "CASE WHEN pmod(hash(id, i, j), 10) = 0 THEN named_struct('id', cast(null as string),"
                + " 'extension', "
                + exts("concat(id, 'g', i, '.', j)", true)
                + ") END"
            : "cast(null as struct<id:string,extension:" + X + ">)";
    return df.withColumn(
            "_birthDate",
            functions.expr(
                "CASE WHEN _birthDate IS NOT NULL THEN named_struct('id', cast(null as string),"
                    + " 'extension', "
                    + exts("concat(id, 'b')", ext)
                    + ", '__start', _birthDate.__start, '__end', _birthDate.__end) END"))
        .withColumn(
            "name",
            functions.expr(
                "transform(name, (n, i) -> named_struct('extension', "
                    + exts("concat(id, 'n', i)", ext)
                    + ", 'use', n.use, 'family', n.family, 'given', n.given, '_given',"
                    + " transform(n.given, (g, j) -> "
                    + given
                    + ")))"));
  }

  public static Dataset<Row> encounter(final Dataset<Row> df, final boolean ext) {
    return df.withColumn(
        "location",
        functions.expr(
            "transform(location, (l, i) -> named_struct('extension', "
                + exts("concat(id, 'l', i)", ext)
                + ", 'location', l.location, 'status', l.status, 'period',"
                + " CASE WHEN l.period IS NOT NULL THEN named_struct('extension', "
                + exts("concat(id, 'p', i)", ext)
                + ", 'start', l.period.start, '_start', CASE WHEN l.period._start IS NOT NULL THEN"
                + " named_struct('id', cast(null as string), 'extension', "
                + exts("concat(id, 's', i)", ext)
                + ", '__start', l.period._start.__start, '__end', l.period._start.__end) END)"
                + " END))"));
  }

  public static Dataset<Row> apply(final Dataset<Row> df, final String type, final boolean ext) {
    return "Patient".equals(type) ? patient(df, ext) : encounter(df, ext);
  }
}
