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
import java.util.Objects;


@Component
@Profile("core|unit-test")
@Slf4j
public class ValidateCodingArray implements SqlFunction2<String, WrappedArray<Row>, Boolean> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String FUNCTION_NAME = "validate_coding_array";

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  public ValidateCodingArray(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.BooleanType;
  }

  @Nullable
  @Override
  public Boolean call(@Nullable final String url, @Nullable final WrappedArray<Row> codingArrayRow)
      throws Exception {
    if (url == null || codingArrayRow == null) {
      return null;
    }
    final TerminologyService terminologyService = terminologyServiceFactory.buildService(
        log);
    return Streams.stream(JavaConverters.asJavaIterable(codingArrayRow))
        .filter(Objects::nonNull).anyMatch(codingRow -> {
          final Coding coding = CodingEncoding.decode(codingRow);
          final Parameters parameters = terminologyService.validate(url, coding);
          return parameters != null && parameters.getParameterBool("result");
        });
  }
}
