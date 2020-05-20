/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.query.functions.AbstractAggregateFunction.thisValuePresentInDataset;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.query.parsing.FhirPathTypeSqlHelper;
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
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#membership">Membership</a>
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
    if (collection.getFhirPathType() == null || element.getFhirPathType() == null || !collection
        .getFhirPathType().equals(element.getFhirPathType())) {
      throw new InvalidRequestException(
          "Operands are of incompatible types: " + input.getExpression());
    }

    FhirPathTypeSqlHelper sqlHelper = FhirPathTypeSqlHelper
        .forType(element.getFhirPathType());

    // Create a new dataset which joins left and right and aggregates on the resource ID based upon
    // whether the left expression is within the set of values in the right expression.
    Dataset<Row> elementDataset = element.getDataset();
    Dataset<Row> collectionDataset = collection.getDataset();

    Column collectionIdColumn = collection.getIdColumn();
    Column collectionColumn = collection.getValueColumn();
    Column elementColumn =
        element.isLiteral()
        ? sqlHelper.getLiteralColumn(element)
        : element.getValueColumn();
    Column elementIdColumn = element.getIdColumn();

    Dataset<Row> membershipDataset = element.isLiteral()
                                     ? collectionDataset
                                     : collectionDataset.join(elementDataset,
                                         collectionIdColumn.equalTo(elementIdColumn),
                                         "left_outer");

    // If the left-hand side of the operator (element) is empty, the result is empty. If the
    // right-hand side (collection) is empty, the result is false. Otherwise, a Boolean is returned
    // based on whether the element is present in the collection, using equality semantics.
    Column equalityColumn =
        when(elementColumn.isNull(), lit(null)).when(collectionColumn.isNull(), lit(false))
            .otherwise(sqlHelper.getEquality().apply(collectionColumn, elementColumn));

    // In order to reduce the result to a single Boolean, we take the max of the boolean equality
    // values, aggregated by the resource ID.
    // Aliasing of equality column here is necessary as otherwise it cannot be resolved in the
    // aggregation.
    membershipDataset = membershipDataset.withColumn("equality", equalityColumn);

    // If the operator is being executed within a `$this` context, we need to preserve the input 
    // value column when we do the aggregation.
    ParsedExpression thisContext = input.getContext().getThisContext();
    Column[] groupBy;
    int selectionStart;
    if (thisValuePresentInDataset(membershipDataset, thisContext)) {
      groupBy = new Column[]{thisContext.getValueColumn(), collectionIdColumn};
      selectionStart = 1;
    } else {
      groupBy = new Column[]{collectionIdColumn};
      selectionStart = 0;
    }

    membershipDataset =
        membershipDataset.groupBy(groupBy).agg(max(membershipDataset.col("equality")));
    Column idColumn = membershipDataset.col(membershipDataset.columns()[selectionStart]);
    Column valueColumn = membershipDataset.col(membershipDataset.columns()[selectionStart + 1]);

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
