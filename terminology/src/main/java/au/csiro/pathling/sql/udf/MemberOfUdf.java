package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.decodeOneOrMany;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.validCodings;

import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;

@Slf4j
public class MemberOfUdf implements SqlFunction,
    SqlFunction2<Object, String, Boolean> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String FUNCTION_NAME = "member_of";

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  protected MemberOfUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.BooleanType;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Nullable
  @Override
  public Boolean call(@Nullable final Object codingRowOrArray, @Nullable final String url)
      throws Exception {
    return doCall(decodeOneOrMany(codingRowOrArray), url);
  }

  @Nullable
  protected Boolean doCall(@Nullable final Stream<Coding> codings, @Nullable final String url) {
    if (url == null || codings == null) {
      return null;
    }
    final TerminologyService2 terminologyService = terminologyServiceFactory.buildService2();
    return validCodings(codings)
        .anyMatch(coding -> terminologyService.validate(url, coding));
  }
}
