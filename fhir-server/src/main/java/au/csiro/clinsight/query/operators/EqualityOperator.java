/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.operators;

import static au.csiro.clinsight.utilities.Strings.md5Short;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Provides the functionality of the family of equality operators within FHIRPath, i.e. = and !=.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#equality">http://hl7.org/fhirpath/2018Sep/index.html#equality</a>
 */
public class EqualityOperator implements BinaryOperator {

  private String operator;

  public EqualityOperator(String operator) {
    this.operator = operator;
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull BinaryOperatorInput input) {
    validateInput(input);
    ParsedExpression left = input.getLeft();
    ParsedExpression right = input.getRight();
    boolean leftIsLiteral = left.getLiteralValue() != null;
    boolean rightIsLiteral = right.getLiteralValue() != null;
    String hash = md5Short(input.getExpression());

    // Check that at least one of the operands contains a Dataset.
    if (leftIsLiteral && rightIsLiteral) {
      throw new InvalidRequestException(
          "Equality operator cannot accept two literal values: " + input.getExpression());
    }

    // Create columns for the left and right expressions, based upon whether they are literals or
    // not.
    Column leftColumn = leftIsLiteral
        ? lit(left.getJavaLiteralValue())
        : left.getDataset().col(left.getDatasetColumn());
    Column rightColumn = rightIsLiteral
        ? lit(right.getJavaLiteralValue())
        : right.getDataset().col(right.getDatasetColumn());

    // Based on the type of operator, create the correct column expression. These expressions are
    // written to take account of the fact that an equality expression involving null will always
    // be null in Spark, whereas in FHIRPath { } = { } should be true and 'foo' = { } should be
    // false.
    Column equality = null;
    switch (operator) {
      case "=":
        equality = when(leftColumn.isNull().and(rightColumn.isNull()), true)
            .when(leftColumn.isNull().or(rightColumn.isNull()), false)
            .otherwise(leftColumn.equalTo(rightColumn));
        break;
      case "!=":
        equality = when(leftColumn.isNull().and(rightColumn.isNull()), false)
            .when(leftColumn.isNull().or(rightColumn.isNull()), true)
            .otherwise(leftColumn.notEqual(rightColumn));
        break;
      default:
        assert false : "Unsupported equality operator encountered: " + operator;
    }

    // Update the dataset to select the new equality column.
    String datasetColumn = leftIsLiteral ? right.getDatasetColumn() : left.getDatasetColumn();
    Dataset<Row> dataset = leftIsLiteral ? right.getDataset() : left.getDataset();
    Column idColumn = dataset.col(datasetColumn + "_id").alias(hash + "_id");
    Column valueColumn = equality.alias(hash);
    // If both expressions have a dataset, we will need to join them.
    if (!leftIsLiteral && !rightIsLiteral) {
      dataset = dataset.join(right.getDataset(), dataset.col(left.getDatasetColumn() + "_id")
          .equalTo(right.getDataset().col(right.getDatasetColumn() + "_id")));
    }
    dataset = dataset.select(idColumn, valueColumn);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FhirType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(dataset);
    result.setDatasetColumn(hash);
    return result;
  }

  private void validateInput(BinaryOperatorInput input) {
    ParsedExpression left = input.getLeft();
    ParsedExpression right = input.getRight();
    boolean leftIsLiteral = left.getLiteralValue() != null;
    boolean rightIsLiteral = right.getLiteralValue() != null;

    // Check that at least one of the operands contains a Dataset.
    if (leftIsLiteral && rightIsLiteral) {
      throw new InvalidRequestException(
          "Equality operator cannot accept two literal values: " + input.getExpression());
    }

    if (!left.isSingular()) {
      throw new InvalidRequestException(
          "Left operand to " + operator + " operator must be singular: " + left.getFhirPath());
    }
    if (!right.isSingular()) {
      throw new InvalidRequestException(
          "Right operand to " + operator + " operator must be singular: " + right.getFhirPath());
    }
    if (!left.isPrimitive()) {
      throw new InvalidRequestException(
          "Left operand to " + operator + " operator must be primitive: " + left.getFhirPath());
    }
    if (!right.isPrimitive()) {
      throw new InvalidRequestException(
          "Right operand to " + operator + " operator must be primitive: " + right.getFhirPath());
    }
    if (left.getFhirPathType() != right.getFhirPathType()) {
      throw new InvalidRequestException(
          "Left and right operands within equality expression must be of same type: " + input
              .getExpression());
    }
  }
}
