/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.PRIMITIVE;
import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.RESOURCE;

import au.csiro.clinsight.fhir.definitions.ElementDefinition;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.PathTraversal;
import au.csiro.clinsight.fhir.definitions.exceptions.ElementNotKnownException;
import au.csiro.clinsight.fhir.definitions.exceptions.ResourceNotKnownException;
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

  public ParseResult invoke(@Nonnull ParseResult input, @Nonnull String member) {
    // Check that we aren't trying to get to a member of a primitive type.
    if (input.getPathTraversal().getType() == PRIMITIVE) {
      throw new InvalidRequestException("Attempt to invoke member on primitive type");
    }

    ParseResult result = new ParseResult();
    // The new expression is the old expression and the new expression, separated by the path
    // selection operator.
    result.setFhirPath(input.getFhirPath() + "." + member);
    // Inherit all joins and from tables from the invoker.
    result.getJoins().addAll(input.getJoins());
    result.getFromTables().addAll(input.getFromTables());

    // Resolve the path of the expression and add it to the result.
    try {
      result.setPathTraversal(PathResolver.resolvePath(result.getFhirPath()));
    } catch (ResourceNotKnownException | ElementNotKnownException e) {
      throw new InvalidRequestException(e.getMessage());
    }

    // Work out the SQL expression.
    if (result.getPathTraversal().getType() == RESOURCE) {
      String sqlExpression = result.getFhirPath().toLowerCase();
      result.setSql(sqlExpression);
      result.getFromTables().add(sqlExpression);
    } else {
      String sqlExpression = input.getSql() + "." + member;
      result.setSql(sqlExpression);
    }

    // If the final element is the subject of a multi-value traversal, we need to add a join.
    if (result.getPathTraversal().getElementDefinition()
        .equals(result.getPathTraversal().getMultiValueTraversals().getLast())) {
      addJoinForMultiValueTraversal(result,
          result.getPathTraversal().getMultiValueTraversals().getLast());
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
    join.setTableAlias(context.getAliasGenerator().getAlias());
    join.setTargetElement(multiValueTraversal);

    // Work out what the expression to "explode" will need to be, based upon whether there are
    // upstream multi-value traversals required to get to this element.
    String udtfExpression = multiValueTraversal.getPath();
    if (pathTraversal.getMultiValueTraversals().size() > 1) {
      ElementDefinition previousTraversal = pathTraversal.getMultiValueTraversals()
          .get(pathTraversal.getMultiValueTraversals().size() - 2);
      result.getJoins().stream()
          .filter(j -> j.getTargetElement().equals(previousTraversal)).findFirst()
          .ifPresent(join::setDependsUpon);
    }

    // If this is not the first join, record a dependency between this join and the previous one.
    // The expression needs to be rewritten to refer to the alias of the target join.
    if (join.getDependsUpon() != null) {
      udtfExpression = udtfExpression
          .replace(join.getDependsUpon().getTargetElement().getPath(),
              join.getDependsUpon().getTableAlias());
    }

    // Build the SQL expression for the join.
    String joinExpression =
        "LATERAL VIEW OUTER explode(" + udtfExpression + ") " + join.getTableAlias() + " AS "
            + join.getTableAlias();
    join.setSql(joinExpression);

    result.getJoins().add(join);
  }

}
