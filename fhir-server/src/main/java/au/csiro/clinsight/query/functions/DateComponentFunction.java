/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.DATE;
import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.DATE_TIME;
import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.TIME;
import static au.csiro.clinsight.utilities.Strings.md5Short;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Describes the functionality of a group of functions that are used for extracting numeric
 * components from date types.
 *
 * @author John Grimes
 * @see <a href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html">https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html</a>
 */
public class DateComponentFunction implements Function {

  private static final Map<String, String> functionsMap = new HashMap<String, String>() {{
    put("toSeconds", "second");
    put("toMinutes", "minute");
    put("toHours", "hour");
    put("dayOfMonth", "dayofmonth");
    put("dayOfWeek", "dayofweek");
    put("weekOfYear", "weekofyear");
    put("toMonthNumber", "month");
    put("toQuarter", "quarter");
    put("toYear", "year");
  }};
  private static final Set<FhirPathType> supportedTypes = new HashSet<FhirPathType>() {{
    add(DATE);
    add(DATE_TIME);
    add(TIME);
  }};

  private final String functionName;

  public DateComponentFunction(String functionName) {
    this.functionName = functionName;
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    Dataset<Row> prevDataset = inputResult.getDataset();
    String prevColumn = inputResult.getDatasetColumn();
    String hash = md5Short(input.getExpression());

    Column column;
    try {
      // Invoke the Spark SQL function named in the above map, and apply it to the value column from
      // the previous dataset.
      Method sparkSqlFunction = functions.class
          .getMethod(functionsMap.get(functionName), Column.class);
      column = (Column) sparkSqlFunction.invoke(functions.class, prevDataset.col(prevColumn));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(
          "Error occurred when attempting to invoke Spark SQL date component method", e);
    }
    column = column.alias(hash);
    Column idColumn = prevDataset.col(prevColumn + "_id").alias(hash + "_id");
    Dataset<Row> dataset = prevDataset.select(idColumn, column);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.INTEGER);
    result.setFhirType(FhirType.INTEGER);
    result.setPrimitive(true);
    result.setSingular(inputResult.isSingular());
    result.setDataset(dataset);
    result.setDatasetColumn(hash);

    return result;
  }

  private void validateInput(FunctionInput input) {
    if (!input.getArguments().isEmpty()) {
      throw new InvalidRequestException(
          "Arguments can not be passed to " + functionName + " function: " + input.getExpression());
    }

    ParsedExpression inputResult = input.getInput();
    if (!supportedTypes.contains(inputResult.getFhirPathType())) {
      throw new InvalidRequestException(
          "Input to " + functionName + " function is of unsupported type: " + inputResult
              .getFhirPath());
    }
  }

}
