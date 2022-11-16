package au.csiro.pathling.terminology;

import au.csiro.pathling.sql.udf.ValidateCoding;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.MDC;
import javax.annotation.Nonnull;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public interface TerminologyOperations {

  @Value
  class Result {

    @Nonnull
    Dataset<Row> dataset;
    @Nonnull
    Column column;
  }

  @Nonnull
  Result memberOf(@Nonnull Dataset<Row> dataset, @Nonnull Column value, String valueSetUri);

}


class TerminologyOperationsLegacyImpl implements TerminologyOperations {

  @Nonnull
  final TerminologyServiceFactory terminologyServiceFactory;

  TerminologyOperationsLegacyImpl(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Nonnull
  @Override
  public Result memberOf(@Nonnull final Dataset<Row> dataset, @Nonnull final Column value,
      final String valueSetUri) {

    final Column arrayColumn = when(value.isNotNull(), array(value))
        .otherwise(functions.lit(null));

    // Prepare the data which will be used within the map operation. All of these things must be
    // Serializable.
    final Dataset<Row> resultDataset = TerminologyFunctions.memberOf(arrayColumn, valueSetUri,
        dataset, "result", terminologyServiceFactory, MDC.get("requestId"));
    final Column resultColumn = functions.col("result");
    return new Result(resultDataset, resultColumn);
  }
}

class TerminologyOperationsImpl implements TerminologyOperations {

  @Nonnull
  @Override
  public Result memberOf(@Nonnull final Dataset<Row> dataset, @Nonnull final Column value,
      final String valueSetUri) {
    final Column resultColumn = callUDF(ValidateCoding.FUNCTION_NAME,
        lit(valueSetUri),
        value);
    return new Result(dataset.repartition(8)
        , resultColumn);
  }

}

