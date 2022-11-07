package au.csiro.pathling.sql.udf;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.terminology.TerminologyService;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.shaded.org.apache.curator.shaded.com.google.common.collect.Streams;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Null;
import java.io.Serializable;
import java.util.Objects;
import java.util.stream.Stream;


@Slf4j
abstract public class ValidateCodingBase implements SqlFunction, Serializable {

  private static final long serialVersionUID = 7605853352299165569L;

  @Nonnull
  protected final TerminologyServiceFactory terminologyServiceFactory;

  protected ValidateCodingBase(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.BooleanType;
  }

  @Nullable
  protected Boolean doCall(@Nullable final String url, @Nullable final Stream<Coding> codings) {
    if (url == null || codings == null) {
      return null;
    }
    final TerminologyService terminologyService = terminologyServiceFactory.buildService(
        log);
    return codings
        .filter(Objects::nonNull).anyMatch(coding -> {
          final Parameters parameters = terminologyService.validate(url, coding);
          return isTrue(parameters);
        });
  }

  public static boolean isTrue(final @Nonnull Parameters parameters) {
    return parameters.getParameterBool("result");
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
