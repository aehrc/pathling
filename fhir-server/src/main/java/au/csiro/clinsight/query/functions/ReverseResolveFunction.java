/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.ElementResolver.resolveElement;
import static au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType.REFERENCE;
import static au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType.RESOURCE;
import static au.csiro.clinsight.query.parsing.Join.JoinType.TABLE_JOIN;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.COLLECTION;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.ResolvedElement;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;
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
  public ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments) {
    validateInput(input);
    validateArguments(arguments);
    assert input.getExpression() != null;
    ResolvedElement inputElement = resolveElement(input.getExpression());
    assert inputElement.getType() == RESOURCE;
    ParseResult argument = arguments.get(0);
    assert argument.getExpression() != null;
    ResolvedElement argumentElement = resolveElement(argument.getExpression());
    assert argumentElement.getType() == REFERENCE;
    boolean argumentReferencesResource = argumentElement.getReferenceTypes().stream()
        .anyMatch(typeUrl -> {
          StructureDefinition typeDefinition = ResourceDefinitions.getResourceByUrl(typeUrl);
          assert typeDefinition != null;
          return typeDefinition.getType().equals(inputElement.getTypeCode());
        });
    if (!argumentReferencesResource) {
      throw new InvalidRequestException(
          "Argument to reverseResolve function does not reference input resource type: " + argument
              .getExpression());
    }
    if (argument.getJoins().isEmpty()) {
      resolveJoinlessArgument(input, inputElement, argument, argumentElement);
    } else {
      resolveArgumentWithJoins(input, argument, argumentElement);
    }
    return input;
  }

  private void resolveArgumentWithJoins(ParseResult input, ParseResult argument,
      ResolvedElement argumentElement) {
    LinkedList<String> argumentPathComponents = Strings.tokenizePath(argumentElement.getPath());
    String targetTable = argumentPathComponents.getFirst().toLowerCase();
    String joinAlias = Strings.pathToLowerCamelCase(argumentPathComponents) + "Resolved";
    String rootExpression = Strings.uncapitalize(argumentElement.getPath());
    Join finalJoin = argument.getJoins().last();
    assert finalJoin.getUdtfExpression() != null;
    String referenceExpression = joinAlias + "." + rootExpression
        .replace(finalJoin.getUdtfExpression(), finalJoin.getTableAlias()) + ".reference";
    String joinExpression = "LEFT JOIN (SELECT * FROM " + targetTable + " ";
    joinExpression += argument.getJoins().stream().map(Join::getExpression).collect(
        Collectors.joining(" "));
    joinExpression +=
        ") " + joinAlias + " ON " + input.getSqlExpression() + ".id = " + referenceExpression;
    Join join = new Join(joinExpression, rootExpression, TABLE_JOIN, joinAlias);
    input.setResultType(COLLECTION);
    input.setElementType(RESOURCE);
    input.setElementTypeCode(argumentPathComponents.getFirst());
    input.setExpression(argumentPathComponents.getFirst());
    input.setSqlExpression(joinAlias);
    input.getJoins().add(join);
  }

  private void resolveJoinlessArgument(@Nonnull ParseResult input, ResolvedElement inputElement,
      ParseResult argument, ResolvedElement argumentElement) {
    LinkedList<String> argumentPathComponents = Strings.tokenizePath(argumentElement.getPath());
    List<String> argumentPathTail = argumentPathComponents
        .subList(1, argumentPathComponents.size());
    assert inputElement.getTypeCode() != null;
    String joinAlias = inputElement.getTypeCode().toLowerCase() + argumentPathComponents.getFirst();
    joinAlias += "As" + Strings.pathToUpperCamelCase(argumentPathTail);
    String targetTable = argumentPathComponents.getFirst().toLowerCase();
    assert argument.getSqlExpression() != null;
    String targetExpression =
        argument.getSqlExpression().replace(targetTable, joinAlias) + ".reference";
    String joinExpression =
        "LEFT JOIN " + targetTable + " " + joinAlias + " ON " + input.getSqlExpression() + ".id = "
            + targetExpression;
    Join join = new Join(joinExpression, targetTable, TABLE_JOIN, joinAlias);
    input.setResultType(COLLECTION);
    input.setElementType(RESOURCE);
    input.setElementTypeCode(argumentPathComponents.getFirst());
    input.setExpression(argumentPathComponents.getFirst());
    input.setSqlExpression(joinAlias);
    input.getJoins().add(join);
  }

  private void validateInput(@Nullable ParseResult input) {
    if (input == null) {
      throw new InvalidRequestException("Missing input expression for resolve function");
    }
    if (input.getElementType() != RESOURCE) {
      throw new InvalidRequestException(
          "Input to reverseResolve function must be a Resource: " + input.getExpression()
              + " (" + input.getElementTypeCode() + ")");
    }
  }

  private void validateArguments(@Nonnull List<ParseResult> arguments) {
    if (arguments.size() != 1
        || arguments.get(0).getElementType() != REFERENCE) {
      throw new InvalidRequestException(
          "Argument to reverseResolve function must be a Reference: " + arguments.get(0)
              .getExpression() + " (" + arguments.get(0).getElementTypeCode() + ")");
    }
  }

  @Override
  public void setTerminologyClient(@Nonnull TerminologyClient terminologyClient) {
  }

  @Override
  public void setSparkSession(@Nonnull SparkSession spark) {
  }

}
