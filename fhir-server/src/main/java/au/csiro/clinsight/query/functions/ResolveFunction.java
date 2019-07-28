/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.REFERENCE;
import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.RESOURCE;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.getResourceByUrl;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.isResource;
import static au.csiro.clinsight.query.parsing.Join.JoinType.TABLE_JOIN;

import au.csiro.clinsight.fhir.definitions.ElementDefinition;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.exceptions.ElementNotKnownException;
import au.csiro.clinsight.fhir.definitions.exceptions.ResourceNotKnownException;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
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

  private ExpressionParserContext context;

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull String expression, @Nullable ParseResult input,
      @Nonnull List<ParseResult> arguments) {
    validateInput(input);
    ElementDefinition element = input.getPathTraversal().getElementDefinition();
    String referenceTypeCode;
    if (element.getReferenceTypes().size() > 1) {
      referenceTypeCode = getTypeForPolymorphicReference(input, arguments);
    } else {
      String referenceTypeUrl = element.getReferenceTypes().get(0);
      StructureDefinition referenceTypeDefinition = getResourceByUrl(referenceTypeUrl);
      referenceTypeCode = referenceTypeDefinition.getType();
      if (referenceTypeCode.equals("Resource")) {
        referenceTypeCode = getTypeForPolymorphicReference(input, arguments);
      }
    }

    // Build a new Join object.
    String joinAlias = context.getAliasGenerator().getAlias();
    String joinExpression = "LEFT JOIN " + referenceTypeCode.toLowerCase() + " " + joinAlias
        + " ON " + input.getSql() + ".reference = "
        + joinAlias + ".id";
    Join newJoin = new Join();
    newJoin.setSql(joinExpression);
    newJoin.setJoinType(TABLE_JOIN);
    newJoin.setTableAlias(joinAlias);
    newJoin.setAliasTarget(referenceTypeCode.toLowerCase());
    if (!input.getJoins().isEmpty()) {
      newJoin.setDependsUpon(input.getJoins().last());
    }

    // Build the parse result.
    ParseResult result = new ParseResult();
    result.setFhirPath(expression);
    result.setSql(joinAlias);
    result.getJoins().addAll(input.getJoins());
    result.getJoins().add(newJoin);
    result.setSingular(input.isSingular());

    // Retrieve the path traversal for the result of the expression.
    try {
      result.setPathTraversal(PathResolver.resolvePath(referenceTypeCode));
    } catch (ResourceNotKnownException | ElementNotKnownException e) {
      throw new InvalidRequestException(e.getMessage());
    }

    return result;
  }

  private void validateInput(@Nullable ParseResult input) {
    if (input == null) {
      throw new InvalidRequestException("Missing input expression for resolve function");
    }
    if (input.getPathTraversal().getType() != REFERENCE) {
      throw new InvalidRequestException(
          "Input to resolve function must be a Reference: " + input.getFhirPath());
    }
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

  @Override
  public void setContext(@Nonnull ExpressionParserContext context) {
    this.context = context;
  }

}
