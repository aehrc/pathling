package au.csiro.pathling.sql.udf;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import org.apache.hadoop.shaded.org.apache.curator.shaded.com.google.common.collect.Streams;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Stream;

public final class TerminologyUdfHelpers {

  private TerminologyUdfHelpers() {
    // Utility class
  }

  public static boolean isTrue(final @Nonnull Parameters parameters) {
    return parameters.getParameterBool("result");
  }

  @Nullable
  static Row[] encodeMany(@Nullable final Stream<Coding> codings) {
    return codings != null
           ? codings.map(CodingEncoding::encode).toArray(Row[]::new)
           : null;
  }

  @Nullable
  public static Stream<Coding> decodeOne(final @Nullable Row codingRow) {
    return codingRow != null
           ? Stream.of(CodingEncoding.decode(codingRow))
           : null;
  }

  @Nullable
  public static Stream<Coding> decodeMany(final @Nullable WrappedArray<Row> codingsRow) {
    return codingsRow != null
           ? Streams.stream(JavaConverters.asJavaIterable(codingsRow)).filter(Objects::nonNull)
               .map(CodingEncoding::decode)
           : null;
  }
}
