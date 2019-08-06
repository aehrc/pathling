/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.REFERENCE;
import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.RESOURCE;
import static au.csiro.clinsight.query.parsing.Join.JoinType.LEFT_JOIN;
import static au.csiro.clinsight.query.parsing.Join.rewriteSqlWithJoinAliases;
import static au.csiro.clinsight.query.parsing.Join.wrapLateralViews;

import au.csiro.clinsight.fhir.definitions.ElementDefinition;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import au.csiro.clinsight.fhir.definitions.exceptions.ElementNotKnownException;
import au.csiro.clinsight.fhir.definitions.exceptions.ResourceNotKnownException;
import au.csiro.clinsight.query.parsing.AliasGenerator;
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

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull ExpressionFunctionInput input) {
    ParseResult inputResult = validateInput(input.getInput());
    ParseResult argument = validateArgument(input.getArguments());
    ElementDefinition inputElement = inputResult.getPathTraversal().getElementDefinition();
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

    ExpressionParserContext context = input.getContext();
    AliasGenerator aliasGenerator = context.getAliasGenerator();

    String joinAlias = aliasGenerator.getAlias();
    String targetResource = Strings.tokenizePath(argument.getFhirPath()).getFirst();
    String targetTable = targetResource.toLowerCase();
    String joinExpression;

    SortedSet<Join> upstreamJoins = argument.getJoins();
    if (upstreamJoins.isEmpty()) {
      String targetExpression = argument.getSql().replace(targetTable, joinAlias) + ".reference";
      joinExpression =
          "LEFT JOIN " + targetTable + " " + joinAlias + " ON " + inputResult.getSql() + ".id = "
              + targetExpression;
      // If there is a filter to be applied as part of this invocation, add an extra join condition
      // in here.
      if (input.getFilter() != null) {
        String filter = input.getFilter()
            .replaceAll("(?<=\\b)" + targetTable + "(?=\\b)", joinAlias);
        joinExpression += " AND " + filter;
      }
    } else {
      // If there is a filter to be applied as part of this invocation, add in its join dependencies.
      if (input.getFilter() != null) {
        upstreamJoins.addAll(input.getFilterJoins());
      }
      joinExpression = "LEFT JOIN (SELECT * FROM " + targetTable + " ";
      joinExpression += upstreamJoins.stream().map(Join::getSql)
          .collect(Collectors.joining(" "));

      // If there is a filter, add in a where clause to limit the scope of this query.
      if (input.getFilter() != null) {
        String filter = rewriteSqlWithJoinAliases(input.getFilter(), upstreamJoins);
        joinExpression += " WHERE " + filter;
      }

      String targetExpression = rewriteSqlWithJoinAliases(argument.getSql() + ".reference",
          upstreamJoins);
      joinExpression +=
          ") " + joinAlias + " ON " + inputResult.getSql() + ".id = " + joinAlias + "."
              + targetExpression;
    }

    // Build the candidate set of joins.
    SortedSet<Join> joins = new TreeSet<>(inputResult.getJoins());
    // If there is a filter to be applied and the join dependencies were not rolled into the new join, they will need to be added here.
    if (input.getFilter() != null && upstreamJoins.isEmpty()) {
      joins.addAll(input.getFilterJoins());
    }
    // Rewrite the new join expression to take account of aliases within the input joins.
    joinExpression = rewriteSqlWithJoinAliases(joinExpression, joins);

    // Build new Join.
    Join reverseResolveJoin = new Join();
    reverseResolveJoin.setSql(joinExpression);
    reverseResolveJoin.setJoinType(LEFT_JOIN);
    reverseResolveJoin.setTableAlias(joinAlias);
    reverseResolveJoin.setAliasTarget(targetTable);
    if (!inputResult.getJoins().isEmpty()) {
      reverseResolveJoin.getDependsUpon().add(inputResult.getJoins().last());
    }
    joins.add(reverseResolveJoin);

    // Wrap any upstream dependencies of our new join which are lateral views.
    joins = wrapLateralViews(joins, reverseResolveJoin, aliasGenerator, context.getFromTable());

    // Build new parse result.
    ParseResult result = new ParseResult();
    result.setFunction(this);
    result.setFunctionInput(input);
    result.setFhirPath(input.getExpression());
    result.setSql(joinAlias);
    result.getJoins().addAll(joins);
    result.setSingular(false);

    // Retrieve the path traversal for the result of the expression.
    try {
      result.setPathTraversal(PathResolver.resolvePath(targetResource));
    } catch (ResourceNotKnownException | ElementNotKnownException e) {
      throw new InvalidRequestException(e.getMessage());
    }

    return result;
  }

  private ParseResult validateInput(@Nullable ParseResult input) {
    if (input == null) {
      throw new InvalidRequestException("Missing input expression for resolve function");
    }
    if (input.getPathTraversal().getType() != RESOURCE) {
      throw new InvalidRequestException(
          "Input to reverseResolve function must be a Resource: " + input.getFhirPath());
    }
    return input;
  }

  private ParseResult validateArgument(@Nonnull List<ParseResult> arguments) {
    ParseResult argument = arguments.get(0);
    if (arguments.size() != 1 || argument.getPathTraversal().getType() != REFERENCE) {
      throw new InvalidRequestException(
          "Argument to reverseResolve function must be a Reference: " + argument.getFhirPath());
    }
    return argument;
  }

}
