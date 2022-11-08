package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.decodeMany;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.encodeMany;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import scala.collection.mutable.WrappedArray;

@Component
@Profile("core|unit-test")
@Slf4j
public class TranslateCodingArray extends TranslateCodingBase implements
    SqlFunction4<WrappedArray<Row>, String, Boolean, String, Row[]> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String FUNCTION_NAME = "translate_coding_array";

  public TranslateCodingArray(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    super(terminologyServiceFactory);
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Nullable
  @Override
  public Row[] call(@Nullable final WrappedArray<Row> codingsArrayRow,
      @Nullable final String conceptMapUri, @Nullable Boolean reverse,
      @Nullable final String equivalences) throws Exception {
    return encodeMany(doCall(decodeMany(codingsArrayRow), conceptMapUri, reverse, equivalences));
  }
}
