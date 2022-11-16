package au.csiro.pathling.sql.udf;

import au.csiro.pathling.terminology.TerminologyServiceFactory;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;

@Slf4j
public class ValidateCoding extends ValidateCodingBase implements
    SqlFunction2<Row, String, Boolean> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String FUNCTION_NAME = "validate_coding";

  public ValidateCoding(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    super(terminologyServiceFactory);
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Nullable
  @Override
  public Boolean call(@Nullable final Row codingRow, @Nullable final String url) throws Exception {
    return doCall(TerminologyUdfHelpers.decodeOne(codingRow), url);
  }
}
