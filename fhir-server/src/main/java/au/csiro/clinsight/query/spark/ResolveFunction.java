/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.fhir.ElementResolver.resolveElement;
import static au.csiro.clinsight.fhir.ResourceDefinitions.getResourceByUrl;
import static au.csiro.clinsight.fhir.ResourceDefinitions.isResource;
import static au.csiro.clinsight.utilities.Strings.pathToLowerCamelCase;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.ResolvedElement;
import au.csiro.clinsight.fhir.ResolvedElement.ResolvedElementType;
import au.csiro.clinsight.query.spark.Join.JoinType;
import au.csiro.clinsight.query.spark.ParseResult.ParseResultType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.StructureDefinition;

/**
 * @author John Grimes
 */
public class ResolveFunction implements ExpressionFunction {

  @Nonnull
  @Override
  public ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments) {
    if (input == null) {
      throw new InvalidRequestException("Missing input expression for resolve function");
    }
    if (input.getElementType() != ResolvedElementType.REFERENCE) {
      throw new InvalidRequestException(
          "Input to resolve function must be a Reference: " + input.getExpression() + " ("
              + input.getElementTypeCode() + ")");
    }
    ResolvedElement element = resolveElement(input.getExpression());
    assert element.getType() == ResolvedElementType.REFERENCE;
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
    String joinExpression = "INNER JOIN " + referenceTypeCode.toLowerCase() + " " + joinAlias
        + " ON " + input.getSqlExpression() + ".reference = "
        + joinAlias + ".id";
    Join join = new Join(joinExpression, referenceTypeCode.toLowerCase(), JoinType.TABLE_JOIN,
        joinAlias);
    if (!input.getJoins().isEmpty()) {
      join.setDependsUpon(input.getJoins().last());
    }
    input.setResultType(ParseResultType.ELEMENT_PATH);
    input.setElementType(ResolvedElementType.RESOURCE);
    input.setElementTypeCode(referenceTypeCode);
    input.setExpression(referenceTypeCode);
    input.setSqlExpression(joinAlias);
    input.getJoins().add(join);
    return input;
  }

  @Nonnull
  private String getTypeForPolymorphicReference(@Nonnull ParseResult input,
      @Nonnull List<ParseResult> arguments) {
    String referenceTypeCode;
    if (arguments.size() == 1) {
      String argument = arguments.get(0).getExpression();
      ResolvedElement argumentElement = resolveElement(argument);
      referenceTypeCode = argumentElement.getTypeCode();
      if (argumentElement.getType() != ResolvedElementType.RESOURCE
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

}
