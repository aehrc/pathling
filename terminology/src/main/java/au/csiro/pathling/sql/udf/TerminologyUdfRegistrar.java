package au.csiro.pathling.sql.udf;

import au.csiro.pathling.terminology.TerminologyServiceFactory;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;

public class TerminologyUdfRegistrar extends SqlFunctionRegistrar {

  public TerminologyUdfRegistrar(@Nonnull TerminologyServiceFactory tsf) {
    super(Collections.emptyList(),
        List.of(new MemberOfUdf(tsf)),
        List.of(new SubsumesUdf(tsf)),
        List.of(new TranslateUdf(tsf)));
  }

  public static void registerUdfs(@Nonnull final SparkSession spark,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    new TerminologyUdfRegistrar(terminologyServiceFactory).configure(spark);
  }
}
