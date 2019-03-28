/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import au.csiro.clinsight.TerminologyClient;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;

/**
 * @author John Grimes
 */
public interface ExpressionFunction {

  @Nonnull
  ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments);

  void setTerminologyClient(@Nonnull TerminologyClient terminologyClient);

  void setSparkSession(@Nonnull SparkSession spark);

}
