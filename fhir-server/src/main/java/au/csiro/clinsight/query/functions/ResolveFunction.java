/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.ElementResolver.resolveElement;
import static au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType.REFERENCE;
import static au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType.RESOURCE;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.getResourceByUrl;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.isResource;
import static au.csiro.clinsight.query.parsing.Join.JoinType.TABLE_JOIN;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.COLLECTION;
import static au.csiro.clinsight.utilities.Strings.pathToLowerCamelCase;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.ResolvedElement;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;
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
  public ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments) {
    validateInput(input);
    assert input.getExpression() != null;
    ResolvedElement element = resolveElement(input.getExpression());
    assert element.getType() == REFERENCE;
    String referenceTypeCode;
    if (element.getReferenceTypes().size() > 1) {
      referenceTypeCode = getTypeForPolymorphicReference(input, arguments);
    } else {
      String referenceTypeUrl = element.getReferenceTypes().get(0);
      StructureDefinition referenceTypeDefinition = getResourceByUrl(referenceTypeUrl);
      assert referenceTypeDefinition != null;
      referenceTypeCode = referenceTypeDefinition.getType();
      if (referenceTypeCode.equals("Resource")) {
        referenceTypeCode = getTypeForPolymorphicReference(input, arguments);
      }
    }

    List<String> pathComponents = tokenizePath(element.getPath());
    String joinAlias = pathToLowerCamelCase(pathComponents);
    String joinExpression = "LEFT JOIN " + referenceTypeCode.toLowerCase() + " " + joinAlias
        + " ON " + input.getSqlExpression() + ".reference = "
        + joinAlias + ".id";
    Join join = new Join(joinExpression, referenceTypeCode.toLowerCase(), TABLE_JOIN, joinAlias);
    if (!input.getJoins().isEmpty()) {
      join.setDependsUpon(input.getJoins().last());
    }
    input.setResultType(COLLECTION);
    input.setElementType(RESOURCE);
    input.setElementTypeCode(referenceTypeCode);
    input.setExpression(referenceTypeCode);
    input.setSqlExpression(joinAlias);
    input.getJoins().add(join);
    return input;
  }

  private void validateInput(@Nullable ParseResult input) {
    if (input == null) {
      throw new InvalidRequestException("Missing input expression for resolve function");
    }
    if (input.getElementType() != REFERENCE) {
      throw new InvalidRequestException(
          "Input to resolve function must be a Reference: " + input.getExpression() + " ("
              + input.getElementTypeCode() + ")");
    }
  }

  @Nonnull
  private String getTypeForPolymorphicReference(@Nonnull ParseResult input,
      @Nonnull List<ParseResult> arguments) {
    String referenceTypeCode;
    if (arguments.size() == 1) {
      String argument = arguments.get(0).getExpression();
      assert argument != null;
      ResolvedElement argumentElement = resolveElement(argument);
      referenceTypeCode = argumentElement.getTypeCode();
      assert referenceTypeCode != null;
      if (argumentElement.getType() != RESOURCE
          || !isResource(referenceTypeCode)) {
        throw new InvalidRequestException(
            "Argument to resolve function must be a base resource type: " + argument + " ("
                + argumentElement.getTypeCode() + ")");
      }
    } else {
      throw new InvalidRequestException(
          "Attempt to resolve polymorphic reference without providing an argument: " + input
              .getExpression());
    }
    return referenceTypeCode;
  }

  @Override
  public void setTerminologyClient(@Nonnull TerminologyClient terminologyClient) {
  }

  @Override
  public void setSparkSession(@Nonnull SparkSession spark) {
  }

  @Override
  public void setDatabaseName(@Nonnull String databaseName) {
  }

}
