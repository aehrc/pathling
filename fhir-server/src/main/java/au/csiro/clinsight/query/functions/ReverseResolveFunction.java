/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.REFERENCE;
import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.RESOURCE;
import static au.csiro.clinsight.query.parsing.Join.JoinType.LATERAL_VIEW;
import static au.csiro.clinsight.query.parsing.Join.JoinType.TABLE_JOIN;
import static au.csiro.clinsight.query.parsing.Join.rewriteSqlWithJoinAliases;
import static au.csiro.clinsight.query.parsing.Join.wrapUpstreamJoins;

import au.csiro.clinsight.fhir.definitions.ElementDefinition;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import au.csiro.clinsight.fhir.definitions.exceptions.ElementNotKnownException;
import au.csiro.clinsight.fhir.definitions.exceptions.ResourceNotKnownException;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.dstu3.model.StructureDefinition;

/**
 * A function for accessing elements of resources which refer to the input resource. The path to the
 * referring element is supplied as an argument.
 *
 * @author John Grimes
 */
public class ReverseResolveFunction implements ExpressionFunction {

  private ExpressionParserContext context;

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull String expression, @Nullable ParseResult input,
      @Nonnull List<ParseResult> arguments) {
    validateInput(input);
    ParseResult argument = validateArgument(arguments);
    ElementDefinition inputElement = input.getPathTraversal().getElementDefinition();
    ElementDefinition argumentElement = argument.getPathTraversal().getElementDefinition();

    // Check that the subject resource type of the argument matches that of the input.
    boolean argumentReferencesResource = argumentElement.getReferenceTypes().stream()
        .anyMatch(typeUrl -> {
          StructureDefinition typeDefinition = ResourceDefinitions.getResourceByUrl(typeUrl);
          assert typeDefinition != null;
          return typeDefinition.getType().equals(inputElement.getTypeCode());
        });
    if (!argumentReferencesResource) {
      throw new InvalidRequestException(
          "Argument to reverseResolve function does not reference input resource type: " + argument
              .getFhirPath());
    }

    return buildResult(expression, input, argument);
  }

  @Nonnull
  private ParseResult buildResult(@Nonnull String expression, @Nonnull ParseResult input,
      @Nonnull ParseResult argument) {
    String joinAlias = context.getAliasGenerator().getAlias();
    String targetResource = Strings.tokenizePath(argument.getFhirPath()).getFirst();
    String targetTable = targetResource.toLowerCase();
    String joinExpression;
    if (argument.getJoins().isEmpty()) {
      String targetExpression = argument.getSql().replace(targetTable, joinAlias) + ".reference";
      joinExpression =
          "LEFT JOIN " + targetTable + " " + joinAlias + " ON " + input.getSql() + ".id = "
              + targetExpression;
    } else {
      joinExpression = "LEFT JOIN (SELECT * FROM " + targetTable + " ";
      joinExpression += argument.getJoins().stream().map(Join::getSql)
          .collect(Collectors.joining(" "));
      String targetExpression = rewriteSqlWithJoinAliases(argument.getSql() + ".reference",
          argument.getJoins());
      joinExpression += ") " + joinAlias + " ON " + input.getSql() + ".id = " + joinAlias + "."
          + targetExpression;
    }

    // Build the candidate set of joins.
    SortedSet<Join> joins = new TreeSet<>(input.getJoins());
    // If the input has joins and the last one is a lateral view, we will need to wrap the upstream
    // joins. This is because Spark SQL does not currently allow a table join to follow a lateral
    // view within a query.
    if (!joins.isEmpty() && joins.last().getJoinType() == LATERAL_VIEW) {
      SortedSet<Join> wrappedJoins = wrapUpstreamJoins(joins,
          context.getAliasGenerator().getAlias(), context.getFromTable());
      joins.clear();
      joins.addAll(wrappedJoins);
    }
    // Rewrite the new join expression to take account of aliases within the input joins.
    joinExpression = rewriteSqlWithJoinAliases(joinExpression, joins);

    // Build new Join.
    Join join = new Join();
    join.setSql(joinExpression);
    join.setJoinType(TABLE_JOIN);
    join.setTableAlias(joinAlias);
    join.setAliasTarget(targetTable);
    if (!input.getJoins().isEmpty()) {
      join.setDependsUpon(input.getJoins().last());
    }
    joins.add(join);

    // Build new parse result.
    ParseResult result = new ParseResult();
    result.setFhirPath(expression);
    result.setSql(joinAlias);
    result.getJoins().addAll(joins);
    result.setSingular(input.isSingular());

    // Retrieve the path traversal for the result of the expression.
    try {
      result.setPathTraversal(PathResolver.resolvePath(targetResource));
    } catch (ResourceNotKnownException | ElementNotKnownException e) {
      throw new InvalidRequestException(e.getMessage());
    }

    return result;
  }

  private void validateInput(@Nullable ParseResult input) {
    if (input == null) {
      throw new InvalidRequestException("Missing input expression for resolve function");
    }
    if (input.getPathTraversal().getType() != RESOURCE) {
      throw new InvalidRequestException(
          "Input to reverseResolve function must be a Resource: " + input.getFhirPath());
    }
  }

  private ParseResult validateArgument(@Nonnull List<ParseResult> arguments) {
    ParseResult argument = arguments.get(0);
    if (arguments.size() != 1 || argument.getPathTraversal().getType() != REFERENCE) {
      throw new InvalidRequestException(
          "Argument to reverseResolve function must be a Reference: " + argument.getFhirPath());
    }
    return argument;
  }

  @Override
  public void setContext(@Nonnull ExpressionParserContext context) {
    this.context = context;
  }

}
