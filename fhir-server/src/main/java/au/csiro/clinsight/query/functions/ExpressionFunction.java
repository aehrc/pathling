/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.query.parsing.ParseResult;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;

/**
 * A function that is supported for use within FHIRPath expressions. The input is the expression
 * that invoked the function (i.e. on the left hand side of the period), and the arguments are
 * passed within parentheses.
 *
 * A function can optionally accept a terminology server and/or a Spark Session in order to augment
 * its behaviour. Functions that don't need these should just implement do-nothing methods.
 *
 * @author John Grimes
 */
public interface ExpressionFunction {

  @Nonnull
  ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments);

  void setTerminologyClient(@Nonnull TerminologyClient terminologyClient);

  void setSparkSession(@Nonnull SparkSession spark);

  void setDatabaseName(@Nonnull String databaseName);

}
