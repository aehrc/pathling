package au.csiro.pathling.sql.udf;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("core")
@Slf4j
public class ValidateCoding implements SqlFunction2<String, Row, Boolean> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String FUNCTION_NAME = "validate_coding";

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  public ValidateCoding(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
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
  public Boolean call(@Nullable final String url, @Nullable final Row codingRow) throws Exception {
    if (url == null || codingRow == null) {
      return null;
    }
    final Coding coding = CodingEncoding.decode(codingRow);
    final Parameters parameters = terminologyServiceFactory.buildService(log).validate(url, coding);
    if (parameters == null) {
      return null;
    }
    return parameters.getParameterBool("result");
  }

}
