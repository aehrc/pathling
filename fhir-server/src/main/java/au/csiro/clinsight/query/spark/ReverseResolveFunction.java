/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.fhir.ElementResolver.resolveElement;

import au.csiro.clinsight.fhir.ResolvedElement;
import au.csiro.clinsight.fhir.ResolvedElement.ResolvedElementType;
import au.csiro.clinsight.fhir.ResourceDefinitions;
import au.csiro.clinsight.query.spark.Join.JoinType;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.dstu3.model.StructureDefinition;

/**
 * @author John Grimes
 */
public class ReverseResolveFunction implements ExpressionFunction {

  @Nonnull
  @Override
  public ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments) {
    if (input == null) {
      throw new InvalidRequestException("Missing input expression for resolve function");
    }
    if (input.getResultType() != ResolvedElementType.RESOURCE) {
      throw new InvalidRequestException(
          "Input to reverseResolve function must be a Resource: " + input.getFhirPathExpression()
              + " (" + input.getResultTypeCode() + ")");
    }
    if (arguments.size() != 1
        || arguments.get(0).getResultType() != ResolvedElementType.REFERENCE) {
      throw new InvalidRequestException(
          "Argument to reverseResolve function must be a Reference: " + arguments.get(0)
              .getFhirPathExpression() + " (" + arguments.get(0).getResultTypeCode() + ")");
    }
    ResolvedElement inputElement = resolveElement(input.getFhirPathExpression());
    assert inputElement.getType() == ResolvedElementType.RESOURCE;
    ResolvedElement argumentElement = resolveElement(arguments.get(0).getFhirPathExpression());
    assert argumentElement.getType() == ResolvedElementType.REFERENCE;
    boolean argumentReferencesResource = argumentElement.getReferenceTypes().stream()
        .anyMatch(typeUrl -> {
          StructureDefinition typeDefinition = ResourceDefinitions.getResourceByUrl(typeUrl);
          assert typeDefinition != null;
          return typeDefinition.getType().equals(inputElement.getTypeCode());
        });
    if (!argumentReferencesResource) {
      throw new InvalidRequestException(
          "Argument to reverseResolve function does not reference input resource type: " + arguments
              .get(0).getFhirPathExpression());
    }
    LinkedList<String> argumentPathComponents = Strings.tokenizePath(argumentElement.getPath());
    List<String> argumentPathTail = argumentPathComponents
        .subList(1, argumentPathComponents.size());
    String joinAlias = inputElement.getTypeCode().toLowerCase() + argumentPathComponents.getFirst();
    joinAlias += "As" + Strings.pathToUpperCamelCase(argumentPathTail);
    String joinExpression =
        "INNER JOIN " + argumentPathComponents.getFirst().toLowerCase() + " " + joinAlias + " ON "
            + input.getSqlExpression() + ".id = " + arguments.get(0).getSqlExpression()
            + ".reference";
    Join join = new Join(joinExpression, argumentPathComponents.getFirst().toLowerCase(),
        JoinType.TABLE_JOIN, joinAlias);
    if (!input.getJoins().isEmpty()) {
      join.setDependsUpon(input.getJoins().last());
    }
    input.setResultType(ResolvedElementType.RESOURCE);
    input.setResultTypeCode(argumentPathComponents.getFirst());
    input.setFhirPathExpression(argumentPathComponents.getFirst());
    input.setSqlExpression(joinAlias);
    input.getJoins().add(join);
    return input;
  }
}
