package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.decodeOne;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.encodeMany;

import au.csiro.pathling.terminology.TerminologyServiceFactory;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;

@Slf4j
public class TranslateCoding extends TranslateCodingBase implements
    SqlFunction4<Row, String, Boolean, String, Row[]> {

  private static final long serialVersionUID = 7605853352299165569L;
  public static final String FUNCTION_NAME = "translate_coding";

  public TranslateCoding(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    super(terminologyServiceFactory);
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Nullable
  @Override
  public Row[] call(@Nullable final Row codingRow, @Nullable final String conceptMapUri,
      @Nullable final Boolean reverse, @Nullable final String equivalences) {
    return encodeMany(doCall(decodeOne(codingRow), conceptMapUri, reverse, equivalences));
  }
}
