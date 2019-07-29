/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.query.parsing.Join.JoinType.MEMBERSHIP_JOIN;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.BOOLEAN;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.CODING;
import static au.csiro.clinsight.utilities.Strings.quote;

import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.SortedSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.dstu3.model.Coding;

/**
 * An expression that tests whether the expression on the left-hand side is in the collection
 * described by the expression on the right hand side.
 *
 * This executes per the logic for "in" by default, switch the left and the right operands to use
 * this for "contains".
 *
 * @author John Grimes
 */
public class MembershipExpression {

  private final ExpressionParserContext context;

  public MembershipExpression(ExpressionParserContext context) {
    this.context = context;
  }

  @Nonnull
  public ParseResult invoke(@Nonnull String expression, @Nullable ParseResult left,
      @Nonnull ParseResult right) {
    validateLeftOperand(left);
    validateRightOperand(right);

    // Build a select expression which tests whether there is a code on the right-hand side of the
    // left join, returning a boolean.
    String resourceTable = context.getFromTable();
    String selectExpression;

    if (left.getResultType() == CODING && left.getLiteralValue() == null) {
      // If the left expression is a singular Coding expression, use equality on the system and code
      // components of the two expressions.
      selectExpression =
          "SELECT " + resourceTable + ".id, IFNULL(MAX(" + right.getSql() + ".system = "
              + left.getSql() + ".system AND " + right.getSql() + ".code = "
              + left.getSql() + ".code), FALSE) AS result";
    } else if (left.getResultType() == CODING && left.getLiteralValue() != null) {
      // If the left expression is a Coding literal, extract the system and code components from the
      // literal and inject them as literal values into the SQL.
      Coding literalValue = (Coding) left.getLiteralValue();
      selectExpression =
          "SELECT " + resourceTable + ".id, IFNULL(MAX(" + right.getSql() + ".system = "
              + quote(literalValue.getSystem()) + " AND " + right.getSql() + ".code = "
              + quote(literalValue.getCode()) + "), FALSE) AS result";
    } else if (left.getResultType() == null && left.getPathTraversal().getElementDefinition()
        .getTypeCode().equals("CodeableConcept")) {
      // If the left expression is a singular CodeableConcept, traverse to the `coding` member, then
      // use equality on the system and code.
      left = new MemberInvocation(context).invoke(left.getFhirPath() + ".coding", left);
      selectExpression =
          "SELECT " + resourceTable + ".id, IFNULL(MAX(" + right.getSql() + ".system = "
              + left.getSql() + ".system AND " + right.getSql() + ".code = "
              + left.getSql() + ".code), FALSE) AS result";
    } else {
      // If the left expression is a singular primitive expression, use simple equality, leveraging
      // the SQL representation that the parser already added to the result.
      selectExpression =
          "SELECT " + resourceTable + ".id, IFNULL(MAX(" + right.getSql() + " = "
              + left.getSql() + "), FALSE) AS result";
    }
    // TODO: Deal with versions within Codings and CodeableConcepts.

    // Get the set of upstream joins.
    SortedSet<Join> upstreamJoins = left.getJoins();
    upstreamJoins.addAll(right.getJoins());

    // Build a SQL expression representing the new subquery that provides the result of the membership test.
    String subqueryAlias = context.getAliasGenerator().getAlias();
    String subquery = "LEFT JOIN (";
    subquery += selectExpression;
    subquery += "FROM " + resourceTable;
    subquery += upstreamJoins.stream().map(Join::getSql).collect(Collectors.joining(" "));
    subquery += ") " + subqueryAlias + " ON " + resourceTable + ".id = " + subqueryAlias + ".id";

    // Create a new Join that represents the join to the new subquery.
    Join newJoin = new Join();
    newJoin.setSql(subquery);
    newJoin.setJoinType(MEMBERSHIP_JOIN);
    newJoin.setTableAlias(subqueryAlias);

    // Build up the new result.
    ParseResult result = new ParseResult();
    result.setFhirPath(expression);
    result.setSql(newJoin.getTableAlias() + ".result");
    result.getJoins().add(newJoin);
    result.setResultType(BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(true);

    return result;
  }

  private void validateLeftOperand(@Nullable ParseResult left) {
    if (left == null) {
      throw new InvalidRequestException("Missing operand for membership expression");
    }
    if (!left.isSingular()) {
      throw new InvalidRequestException(
          "Operand in membership expression must evaluate to a single value: " + left
              .getFhirPath());
    }
    String typeCode = left.getPathTraversal().getElementDefinition().getTypeCode();
    if (!left.isPrimitive() || left.getResultType() == CODING || typeCode
        .equals("CodeableConcept")) {
      throw new InvalidRequestException(
          "Operand in membership expression must be primitive, Coding or CodeableConcept: " + left
              .getFhirPath());
    }
  }

  private void validateRightOperand(@Nullable ParseResult right) {
    if (right == null) {
      throw new InvalidRequestException("Missing operand for membership expression");
    }
    if (right.isSingular()) {
      throw new InvalidRequestException(
          "Operand in membership expression must evaluate to a collection: " + right
              .getFhirPath());
    }
    String typeCode = right.getPathTraversal().getElementDefinition().getTypeCode();
    if (!right.isPrimitive() || right.getResultType() == CODING || typeCode
        .equals("CodeableConcept")) {
      throw new InvalidRequestException(
          "Operand in membership expression must be primitive, Coding or CodeableConcept: " + right
              .getFhirPath());
    }
  }

}
