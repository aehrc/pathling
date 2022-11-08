package au.csiro.pathling.sql.udf;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.terminology.TerminologyService;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
          return TerminologyUdfHelpers.isTrue(parameters);
        });
  }

}
