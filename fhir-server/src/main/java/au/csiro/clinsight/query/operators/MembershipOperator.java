/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.operators;

import static org.apache.spark.sql.functions.max;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirType;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * An expression that tests whether a singular value is present within a collection.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#collections-2">http://hl7.org/fhirpath/2018Sep/index.html#collections-2</a>
 */
public class MembershipOperator implements BinaryOperator {

  private final String operator;

  public MembershipOperator(String operator) {
    this.operator = operator;
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull BinaryOperatorInput input) {
    // Assign the operands in an order based upon whether this is an "in" or a "contains" operator.
    // The "contains" operator is the reverse of "in".
    ParsedExpression left = input.getLeft();
    ParsedExpression right = input.getRight();
    if (operator.equals("contains")) {
      right = input.getLeft();
      left = input.getRight();
    } else {
      assert operator.equals("in") : "Unsupported membership operator encountered: " + operator;
    }

    // Check that the "left" operand is singular.
    if (!left.isSingular()) {
      throw new InvalidRequestException(
          "Left operand to " + operator + " operator is not singular: " + left.getFhirPath());
    }

    String fhirPath =
        left.getFhirPath() + " " + operator + " " + right.getFhirPath();

    // Create a new dataset which joins left and right and aggregates on the resource ID based upon
    // whether the left expression is within the set of values in the right expression.
    Dataset<Row> leftDataset = left.getDataset();
    Dataset<Row> rightDataset = right.getDataset();
    Column leftIdColumn = leftDataset.col(left.getDatasetColumn() + "_id");
    Column leftColumn = leftDataset.col(left.getDatasetColumn());
    Column rightIdColumn = rightDataset.col(right.getDatasetColumn() + "_id");
    Column rightColumn = rightDataset.col(right.getDatasetColumn());
    String newHash = Strings.md5Short(fhirPath);
    Dataset<Row> equalityDataset = leftDataset
        .join(rightDataset, leftIdColumn.equalTo(rightIdColumn), "left_outer");

    // We take the max of the boolean equality values, aggregated by the resource ID.
    Column equalityColumn = leftColumn.equalTo(rightColumn);
    equalityDataset = equalityDataset.select(leftIdColumn, equalityColumn);
    equalityDataset.groupBy().agg(max(equalityColumn));

    // Rename the columns using the hashed FHIRPath expression.
    equalityDataset.select(leftIdColumn.alias(newHash + "_id"),
        equalityDataset.col(equalityDataset.columns()[1]).alias(newHash));

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(fhirPath);
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FhirType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(equalityDataset);
    result.setDatasetColumn(newHash);
    return result;
  }
}
