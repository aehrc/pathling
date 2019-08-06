/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.REFERENCE;
import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.RESOURCE;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.getResourceByUrl;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.isResource;
import static au.csiro.clinsight.query.parsing.Join.JoinType.LEFT_JOIN;
import static au.csiro.clinsight.query.parsing.Join.rewriteSqlWithJoinAliases;
import static au.csiro.clinsight.query.parsing.Join.wrapLateralViews;

import au.csiro.clinsight.fhir.definitions.ElementDefinition;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.exceptions.ElementNotKnownException;
import au.csiro.clinsight.fhir.definitions.exceptions.ResourceNotKnownException;
import au.csiro.clinsight.query.parsing.AliasGenerator;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.dstu3.model.StructureDefinition;

/**
 * A function for resolving a Reference element in order to access the elements of the target
 * resource. Supports polymorphic references through the use of an argument specifying the target
 * resource type.
 *
 * @author John Grimes
 */
public class ResolveFunction implements ExpressionFunction {

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull ExpressionFunctionInput input) {
    ParseResult inputResult = validateInput(input.getInput());
    ExpressionParserContext context = input.getContext();
    AliasGenerator aliasGenerator = context.getAliasGenerator();

    ElementDefinition element = inputResult.getPathTraversal().getElementDefinition();
    String referenceTypeCode;
    if (element.getReferenceTypes().size() > 1) {
      referenceTypeCode = getTypeForPolymorphicReference(inputResult, input.getArguments());
    } else {
      String referenceTypeUrl = element.getReferenceTypes().get(0);
      StructureDefinition referenceTypeDefinition = getResourceByUrl(referenceTypeUrl);
      referenceTypeCode = referenceTypeDefinition.getType();
      if (referenceTypeCode.equals("Resource")) {
        referenceTypeCode = getTypeForPolymorphicReference(inputResult, input.getArguments());
      }
    }

    // Draft up the new join expression.
    String joinAlias = aliasGenerator.getAlias();
    String joinExpression = "LEFT JOIN " + referenceTypeCode.toLowerCase() + " " + joinAlias
        + " ON " + inputResult.getSql() + ".reference = "
        + joinAlias + ".id";

    // If there is a filter to be applied as part of this invocation, add an extra join condition
    // in here.
    if (input.getFilter() != null) {
      String filter = input.getFilter()
          .replaceAll("(?<=\\b)" + referenceTypeCode.toLowerCase() + "(?=\\b)", joinAlias);
      joinExpression += "AND " + filter;
    }

    // Build the candidate set of joins.
    SortedSet<Join> joins = new TreeSet<>(inputResult.getJoins());

    // If there is a filter to be applied as part of this invocation, add in its join dependencies.
    if (input.getFilter() != null) {
      joins.addAll(input.getFilterJoins());
    }

    // Rewrite the new join expression to take account of aliases within the input joins.
    joinExpression = rewriteSqlWithJoinAliases(joinExpression, joins);

    // Build a new Join object.
    Join resolveJoin = new Join();
    resolveJoin.setSql(joinExpression);
    resolveJoin.setJoinType(LEFT_JOIN);
    resolveJoin.setTableAlias(joinAlias);
    resolveJoin.setAliasTarget(referenceTypeCode.toLowerCase());
    if (!inputResult.getJoins().isEmpty()) {
      resolveJoin.getDependsUpon().add(inputResult.getJoins().last());
    }
    joins.add(resolveJoin);

    // Wrap any upstream dependencies of our new join which are lateral views.
    joins = wrapLateralViews(joins, resolveJoin, aliasGenerator, context.getFromTable());

    // Build the parse result.
    ParseResult result = new ParseResult();
    result.setFunction(this);
    result.setFunctionInput(input);
    result.setFhirPath(input.getExpression());
    result.setSql(joinAlias);
    result.getJoins().addAll(joins);
    result.setSingular(inputResult.isSingular());

    // Retrieve the path traversal for the result of the expression.
    try {
      result.setPathTraversal(PathResolver.resolvePath(referenceTypeCode));
    } catch (ResourceNotKnownException | ElementNotKnownException e) {
      throw new InvalidRequestException(e.getMessage());
    }

    return result;
  }

  private ParseResult validateInput(@Nullable ParseResult input) {
    if (input == null) {
      throw new InvalidRequestException("Missing input expression for resolve function");
    }
    if (input.getPathTraversal().getType() != REFERENCE) {
      throw new InvalidRequestException(
          "Input to resolve function must be a Reference: " + input.getFhirPath());
    }
    return input;
  }

  @Nonnull
  private String getTypeForPolymorphicReference(@Nonnull ParseResult input,
      @Nonnull List<ParseResult> arguments) {
    String referenceTypeCode;
    if (arguments.size() == 1) {
      ParseResult argument = arguments.get(0);
      referenceTypeCode = argument.getPathTraversal().getElementDefinition().getTypeCode();
      if (argument.getPathTraversal().getType() != RESOURCE || !isResource(referenceTypeCode)) {
        throw new InvalidRequestException(
            "Argument to resolve function must be a base resource type: " + argument.getFhirPath());
      }
    } else {
      throw new InvalidRequestException(
          "Attempt to resolve polymorphic reference without providing an argument: " + input
              .getFhirPath());
    }
    return referenceTypeCode;
  }

}
