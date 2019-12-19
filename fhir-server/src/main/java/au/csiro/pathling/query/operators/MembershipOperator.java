/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * An expression that tests whether a singular value is present within a collection.
 *
 * @author John Grimes
 * @see <a href= "http://hl7.org/fhirpath/2018Sep/index.html#collections-2">http://hl7.org/fhirpath/2018Sep/index.html#collections-2</a>
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
    ParsedExpression element = input.getLeft();
    ParsedExpression collection = input.getRight();
    if (operator.equals("contains")) {
      collection = input.getLeft();
      element = input.getRight();
    } else {
      assert operator.equals("in") : "Unsupported membership operator encountered: " + operator;
    }

    // Check that the "left" operand is singular.
    if (!element.isSingular()) {
      throw new InvalidRequestException(
          "Element operand to " + operator + " operator is not singular: " + element.getFhirPath());
    }

    // check that left and right have the same type
    if (!collection.getFhirPathType().equals(element.getFhirPathType())) {
      throw new InvalidRequestException(
          "Operands are of incompatible types: " + input.getExpression());
    }

    // Create a new dataset which joins left and right and aggregates on the resource ID based upon
    // whether the left expression is within the set of values in the right expression.
    Dataset<Row> elementDataset = element.getDataset();
    Dataset<Row> collectionDataset = collection.getDataset();

    Column collectionIdColumn = collection.getIdColumn();
    Column collectionColumn = collection.getValueColumn();
    Column elementColumn =
        element.isLiteral() ? lit(element.getJavaLiteralValue()) : element.getValueColumn();
    Column elementIdColumn = element.getIdColumn();

    Dataset<Row> membershipDataset = element.isLiteral() ? collectionDataset
        : collectionDataset.join(elementDataset, collectionIdColumn.equalTo(elementIdColumn),
            "left_outer");

    // We take the max of the boolean equality values, aggregated by the resource ID.
    Column equalityColumn =
        when(collectionColumn.isNull().or(elementColumn.isNull()), lit(false))
            .otherwise(collectionColumn.equalTo(elementColumn));

    // Aliasing of equality columnn here is necessary as otherwise it cannot be
    // (for whatever reason) resolved in the aggregation
    membershipDataset =
        membershipDataset.select(collectionIdColumn, equalityColumn.alias("equality"));
    membershipDataset =
        membershipDataset.groupBy(collectionIdColumn).agg(max(membershipDataset.col("equality")));
    Column idColumn = membershipDataset.col(membershipDataset.columns()[0]);
    Column valueColumn = membershipDataset.col(membershipDataset.columns()[1]);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(membershipDataset);
    result.setHashedValue(idColumn, valueColumn);
    return result;
  }
}
