/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.PRIMITIVE;
import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.RESOURCE;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.isPrimitive;
import static au.csiro.clinsight.query.parsing.Join.JoinType.LATERAL_VIEW;

import au.csiro.clinsight.fhir.definitions.ElementDefinition;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.PathTraversal;
import au.csiro.clinsight.fhir.definitions.exceptions.ElementNotKnownException;
import au.csiro.clinsight.fhir.definitions.exceptions.ResourceNotKnownException;
import au.csiro.clinsight.query.Mappings;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;

/**
 * Provides the ability to move from one element to its child element, using the path selection
 * notation ".".
 *
 * @author John Grimes
 */
public class MemberInvocation {

  private final ExpressionParserContext context;

  public MemberInvocation(ExpressionParserContext context) {
    this.context = context;
  }

  public ParseResult invoke(@Nonnull String expression, @Nonnull ParseResult input) {
    // Check that we aren't trying to get to a member of a primitive type.
    if (input.getPathTraversal().getType() == PRIMITIVE) {
      throw new InvalidRequestException("Attempt to invoke member on primitive type");
    }

    ParseResult result = new ParseResult();
    result.setFhirPath(expression);
    // Inherit all joins and from tables from the invoker.
    result.getJoins().addAll(input.getJoins());

    // Resolve the path of the expression and add it to the result.
    try {
      String path = input.getPathTraversal().getPath() + "." + result.getFhirPath();
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
      String sqlExpression = input.getSql() + "." + result.getFhirPath();
      result.setSql(sqlExpression);
    }

    // If the final element is the subject of a multi-value traversal, we need to add a join.
    ElementDefinition elementDefinition = pathTraversal.getElementDefinition();
    if (!pathTraversal.getMultiValueTraversals().isEmpty() && elementDefinition
        .equals(pathTraversal.getMultiValueTraversals().getLast())) {
      addJoinForMultiValueTraversal(result, pathTraversal.getMultiValueTraversals().getLast());
    }

    // Check whether we need to mark this result as a primitive, and record its type.
    String typeCode = elementDefinition.getTypeCode();
    if (isPrimitive(typeCode)) {
      result.setPrimitive(true);
      result.setResultType(Mappings.getFhirPathType(typeCode));
    }

    // Check whether we need to mark this result as singular.
    if (pathTraversal.getMultiValueTraversals().size() == 0) {
      result.setSingular(true);
    }
    return result;
  }

  /**
   * Populate an individual join into a ParseResult, based on an individual MultiValueTraversal.
   */
  private void addJoinForMultiValueTraversal(@Nonnull ParseResult result,
      @Nonnull ElementDefinition multiValueTraversal) {
    PathTraversal pathTraversal = result.getPathTraversal();

    // Create a new Join, based upon information from the multi-value traversal.
    Join join = new Join();
    join.setJoinType(LATERAL_VIEW);
    join.setTableAlias(context.getAliasGenerator().getAlias());
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
    joinExpression = Join.rewriteSqlWithJoinAliases(joinExpression, result.getJoins());

    join.setSql(joinExpression);
    result.getJoins().add(join);
  }

}
