/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.PRIMITIVE;
import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.RESOURCE;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.isPrimitive;
import static au.csiro.clinsight.query.parsing.Join.JoinType.LATERAL_VIEW;
import static au.csiro.clinsight.query.parsing.Join.JoinType.LEFT_JOIN;
import static au.csiro.clinsight.query.parsing.Join.rewriteSqlWithJoinAliases;

import au.csiro.clinsight.fhir.definitions.ElementDefinition;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.PathTraversal;
import au.csiro.clinsight.fhir.definitions.exceptions.ElementNotKnownException;
import au.csiro.clinsight.fhir.definitions.exceptions.ResourceNotKnownException;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.query.parsing.ParseResult.FhirPathType;
import au.csiro.clinsight.query.parsing.ParseResult.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Provides the ability to move from one element to its child element, using the path selection
 * notation ".".
 *
 * @author John Grimes
 */
public class MemberInvocation implements ExpressionFunction {

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull ExpressionFunctionInput input) {
    ParseResult inputResult = input.getInput();

    // Check that we aren't trying to get to a member of a primitive type.
    if (inputResult.getPathTraversal().getType() == PRIMITIVE) {
      throw new InvalidRequestException("Attempt to invoke member on primitive type");
    }

    ParseResult result = new ParseResult();
    result.setFunction(this);
    result.setFunctionInput(input);
    result.setFhirPath(input.getExpression());
    // Inherit all joins and from tables from the invoker.
    result.getJoins().addAll(inputResult.getJoins());

    // Resolve the path of the expression and add it to the result.
    try {
      String path = inputResult.getPathTraversal().getPath() + "." + result.getFhirPath();
      result.setPathTraversal(PathResolver.resolvePath(path));
    } catch (ResourceNotKnownException | ElementNotKnownException e) {
      throw new InvalidRequestException(e.getMessage());
    }

    // Work out the SQL expression.
    PathTraversal pathTraversal = result.getPathTraversal();
    if (pathTraversal.getType() == RESOURCE) {
      String sqlExpression = result.getFhirPath().toLowerCase();
      result.setSql(sqlExpression);
    } else {
      String sqlExpression = inputResult.getSql() + "." + result.getFhirPath();
      result.setSql(sqlExpression);
    }

    // If the final element is the subject of a multi-value traversal, we need to add a join.
    ElementDefinition elementDefinition = pathTraversal.getElementDefinition();
    if (!pathTraversal.getMultiValueTraversals().isEmpty() && elementDefinition
        .equals(pathTraversal.getMultiValueTraversals().getLast())) {
      addJoinForMultiValueTraversal(result, pathTraversal.getMultiValueTraversals().getLast(),
          input);
    }

    // Check whether we need to mark this result as a primitive, and record its type.
    String typeCode = elementDefinition.getTypeCode();
    if (isPrimitive(typeCode)) {
      result.setPrimitive(true);
      result.setFhirPathType(FhirPathType.forFhirTypeCode(typeCode));
      result.setFhirType(FhirType.forFhirTypeCode(typeCode));
    } else if (typeCode.equals("Coding")) {
      result.setFhirPathType(FhirPathType.CODING);
    }

    // Check whether we need to mark this result as singular.
    if (pathTraversal.getMultiValueTraversals().size() == 0 && inputResult.isSingular()) {
      result.setSingular(true);
    }
    return result;
  }

  /**
   * Populate an individual join into a ParseResult, based on an individual MultiValueTraversal.
   */
  private void addJoinForMultiValueTraversal(@Nonnull ParseResult result,
      @Nonnull ElementDefinition multiValueTraversal, @Nonnull ExpressionFunctionInput input) {
    PathTraversal pathTraversal = result.getPathTraversal();

    // Create a new Join, based upon information from the multi-value traversal.
    Join join = new Join();
    join.setJoinType(LATERAL_VIEW);
    join.setTableAlias(input.getContext().getAliasGenerator().getAlias());
    join.setTargetElement(multiValueTraversal);

    // Work out what the expression to "explode" will need to be, based upon whether there are
    // upstream multi-value traversals required to get to this element.
    String udtfExpression = result.getSql();
    if (pathTraversal.getMultiValueTraversals().size() > 1) {
      ElementDefinition previousTraversal = pathTraversal.getMultiValueTraversals()
          .get(pathTraversal.getMultiValueTraversals().size() - 2);
      result.getJoins().stream()
          .filter(j ->
              j.getTargetElement() != null && j.getTargetElement().equals(previousTraversal))
          .findFirst()
          .ifPresent(join::setDependsUpon);
    }
    join.setAliasTarget(udtfExpression);

    // If this is not the first join, record a dependency between this join and the previous one.
    if (!result.getJoins().isEmpty()) {
      join.setDependsUpon(result.getJoins().last());
    }

    // Build the SQL expression for the join.
    String joinExpression =
        "LATERAL VIEW OUTER EXPLODE(" + udtfExpression + ") " + join.getTableAlias() + " AS "
            + join.getTableAlias();

    // Rewrite the expression taking into account aliases in the upstream joins.
    joinExpression = rewriteSqlWithJoinAliases(joinExpression, result.getJoins());

    join.setSql(joinExpression);
    result.getJoins().add(join);

    // If there is a filter to be applied, we will need to wrap the joins and add a where clause
    // to the subquery.
    if (input.getFilter() != null) {
      // Add filter joins to the result.
      result.getJoins().addAll(input.getFilterJoins());
      String filter = rewriteSqlWithJoinAliases(input.getFilter(), result.getJoins());

      // Create a new join, which will wrap all previous joins.
      Join wrappedJoins = new Join();
      wrappedJoins.setJoinType(LEFT_JOIN);
      wrappedJoins.setTableAlias(input.getContext().getAliasGenerator().getAlias());
      wrappedJoins.setTargetElement(multiValueTraversal);

      // Build the SQL expression for the join.
      String subqueryAlias = input.getContext().getAliasGenerator().getAlias();
      String resourceTable = input.getContext().getFromTable();
      String wrappedExpression = "LEFT JOIN (";
      wrappedExpression += "SELECT " + resourceTable + ".id, " + join.getTableAlias() + ".* ";
      wrappedExpression += "FROM " + resourceTable + " ";
      wrappedExpression +=
          result.getJoins().stream().map(Join::getSql).collect(Collectors.joining(" ")) + " ";
      wrappedExpression += "WHERE " + filter;
      wrappedExpression +=
          ") " + subqueryAlias + " ON " + resourceTable + ".id = " + subqueryAlias + ".id";
      wrappedJoins.setSql(wrappedExpression);

      // Replace the joins in the result with the wrapped joins.
      result.getJoins().clear();
      result.getJoins().add(wrappedJoins);
    }
  }

}
