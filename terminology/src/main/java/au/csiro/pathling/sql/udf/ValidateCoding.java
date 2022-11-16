package au.csiro.pathling.sql.udf;

import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import scala.collection.mutable.WrappedArray;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
public class ValidateCoding implements SqlFunction,
    SqlFunction2<Object, String, Boolean> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String FUNCTION_NAME = "validate_coding";

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  protected ValidateCoding(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
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
  public Boolean call(@Nullable final Object codingRow, @Nullable final String url)
      throws Exception {
    if (codingRow instanceof WrappedArray<?>) {
      //noinspection unchecked
      return doCall(TerminologyUdfHelpers.decodeMany((WrappedArray<Row>) codingRow), url);
    } else if (codingRow instanceof Row || codingRow == null) {
      return doCall(TerminologyUdfHelpers.decodeOne((Row) codingRow), url);
    } else {
      throw new IllegalArgumentException(
          "struct or array<struct> column expected as the first argument");
    }
  }

  @Nullable
  protected Boolean doCall(@Nullable final Stream<Coding> codings, @Nullable final String url) {
    if (url == null || codings == null) {
      return null;
    }
    final TerminologyService2 terminologyService = terminologyServiceFactory.buildService2();
    return codings
        .filter(Objects::nonNull).anyMatch(coding -> {
          final Parameters parameters = terminologyService.validate(url, coding);
          return TerminologyUdfHelpers.isTrue(parameters);
        });
  }
}
