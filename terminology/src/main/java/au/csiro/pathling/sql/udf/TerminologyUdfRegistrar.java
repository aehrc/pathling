package au.csiro.pathling.sql.udf;

import au.csiro.pathling.terminology.TerminologyServiceFactory;
import org.apache.spark.sql.SparkSession;
import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TerminologyUdfRegistrar extends SqlFunctionRegistrar {

  public TerminologyUdfRegistrar(@Nonnull TerminologyServiceFactory tsf) {
    super(Collections.emptyList(),
        List.of(new ValidateCoding(tsf)),
        Collections.emptyList(),
        Arrays.asList(new TranslateCoding(tsf), new TranslateCodingArray(tsf)));
  }


  public static void registerUdfs(@Nonnull final SparkSession spark,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    new TerminologyUdfRegistrar(terminologyServiceFactory).configure(spark);
  }
}
