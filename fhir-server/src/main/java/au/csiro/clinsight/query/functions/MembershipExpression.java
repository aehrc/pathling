/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.query.parsing.Join.JoinType.LEFT_JOIN;
import static au.csiro.clinsight.query.parsing.Join.rewriteSqlWithJoinAliases;
import static au.csiro.clinsight.query.parsing.ParseResult.FhirPathType.CODING;
import static au.csiro.clinsight.utilities.Strings.singleQuote;

import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.query.parsing.ParseResult.FhirPathType;
import au.csiro.clinsight.query.parsing.ParseResult.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
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
public class MembershipExpression implements ExpressionFunction {

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull ExpressionFunctionInput input) {
    ExpressionParserContext context = input.getContext();
    ParseResult left = validateLeftOperand(input.getInput(), context);
    ParseResult right = validateRightOperand(input.getArguments(), context);

    // Build a select expression which tests whether there is a code on the right-hand side of the
    // left join, returning a boolean.
    String resourceTable = context.getFromTable();
    String selectExpression;

    if (left.getFhirPathType() == CODING && left.getLiteralValue() == null) {
      // If the left expression is a singular Coding expression, use equality on the system and code
      // components of the two expressions.
      selectExpression =
          "SELECT " + resourceTable + ".id, IFNULL(MAX(" + right.getSql() + ".system = "
              + left.getSql() + ".system AND " + right.getSql() + ".code = "
              + left.getSql() + ".code), FALSE) AS result";
    } else if (left.getFhirPathType() == CODING && left.getLiteralValue() != null) {
      // If the left expression is a Coding literal, extract the system and code components from the
      // literal and inject them as literal values into the SQL.
      Coding literalValue = (Coding) left.getLiteralValue();
      selectExpression =
          "SELECT " + resourceTable + ".id, IFNULL(MAX(" + right.getSql() + ".system = "
              + singleQuote(literalValue.getSystem()) + " AND " + right.getSql() + ".code = "
              + singleQuote(literalValue.getCode()) + "), FALSE) AS result";
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
    selectExpression = rewriteSqlWithJoinAliases(selectExpression, upstreamJoins);

    // If there is a filter to be applied as part of this invocation, add in its join dependencies.
    if (input.getFilter() != null) {
      upstreamJoins.addAll(input.getFilterJoins());
    }

    // Build a SQL expression representing the new subquery that provides the result of the membership test.
    String subqueryAlias = context.getAliasGenerator().getAlias();
    String subquery = "LEFT JOIN (";
    subquery += selectExpression + " ";
    subquery += "FROM " + resourceTable + " ";
    subquery += upstreamJoins.stream().map(Join::getSql).collect(Collectors.joining(" ")) + " ";

    // If there is a filter to be applied as part of this invocation, add a where clause in here.
    if (input.getFilter() != null) {
      String filter = rewriteSqlWithJoinAliases(input.getFilter(), upstreamJoins);
      subquery += "WHERE " + filter + " ";
    }

    subquery += "GROUP BY 1";
    subquery += ") " + subqueryAlias + " ON " + resourceTable + ".id = " + subqueryAlias + ".id";

    // Create a new Join that represents the join to the new subquery.
    Join newJoin = new Join();
    newJoin.setSql(subquery);
    newJoin.setJoinType(LEFT_JOIN);
    newJoin.setTableAlias(subqueryAlias);

    // Build up the new result.
    ParseResult result = new ParseResult();
    result.setFunction(this);
    result.setFunctionInput(input);
    result.setFhirPath(input.getExpression());
    result.setSql(newJoin.getTableAlias() + ".result");
    result.getJoins().add(newJoin);
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FhirType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(true);

    return result;
  }

  private ParseResult validateLeftOperand(@Nullable ParseResult left,
      ExpressionParserContext context) {
    if (left == null) {
      throw new InvalidRequestException("Missing operand for membership expression");
    }
    if (!left.isSingular()) {
      throw new InvalidRequestException(
          "Operand in membership expression must evaluate to a single value: " + left
              .getFhirPath());
    }
    boolean isCodeableConcept =
        left.getPathTraversal() != null && left.getPathTraversal().getElementDefinition()
            .getTypeCode().equals("CodeableConcept");
    if (!(left.isPrimitive() || left.getFhirPathType() == CODING || isCodeableConcept)) {
      throw new InvalidRequestException(
          "Operand in membership expression must be primitive, Coding or CodeableConcept: " + left
              .getFhirPath());
    }

    // If the left expression is a singular CodeableConcept, traverse to the `coding` member.
    if (isCodeableConcept) {
      ExpressionFunctionInput memberInvocationInput = new ExpressionFunctionInput();
      memberInvocationInput.setExpression("coding");
      memberInvocationInput.setInput(left);
      memberInvocationInput.setContext(context);
      left = new MemberInvocation().invoke(memberInvocationInput);
    }

    return left;
  }

  private ParseResult validateRightOperand(@Nullable List<ParseResult> arguments,
      ExpressionParserContext context) {
    assert arguments != null && arguments.size() == 1;
    ParseResult right = arguments.get(0);
    if (right == null) {
      throw new InvalidRequestException("Missing operand for membership expression");
    }
    if (right.isSingular()) {
      throw new InvalidRequestException(
          "Operand in membership expression must evaluate to a collection: " + right
              .getFhirPath());
    }
    boolean isCodeableConcept =
        right.getPathTraversal() != null && right.getPathTraversal().getElementDefinition()
            .getTypeCode().equals("CodeableConcept");
    if (!(right.isPrimitive() || right.getFhirPathType() == CODING || isCodeableConcept)) {
      throw new InvalidRequestException(
          "Operand in membership expression must be primitive, Coding or CodeableConcept: " + right
              .getFhirPath());
    }

    // If the right expression is a singular CodeableConcept, traverse to the `coding` member.
    if (isCodeableConcept) {
      ExpressionFunctionInput memberInvocationInput = new ExpressionFunctionInput();
      memberInvocationInput.setExpression("coding");
      memberInvocationInput.setInput(right);
      memberInvocationInput.setContext(context);
      right = new MemberInvocation().invoke(memberInvocationInput);
    }

    return right;
  }

}
