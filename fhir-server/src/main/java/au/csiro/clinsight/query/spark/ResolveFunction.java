/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.fhir.ElementResolver.resolveElement;
import static au.csiro.clinsight.fhir.ResourceDefinitions.getResourceByUrl;
import static au.csiro.clinsight.fhir.ResourceDefinitions.isResource;

import au.csiro.clinsight.fhir.ResolvedElement;
import au.csiro.clinsight.fhir.ResolvedElement.ResolvedElementType;
import au.csiro.clinsight.query.spark.Join.JoinType;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.dstu3.model.StructureDefinition;

/**
 * @author John Grimes
 */
public class ResolveFunction implements ExpressionFunction {

  @Override
  public ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments) {
    if (input == null) {
      throw new InvalidRequestException("Missing input expression for resolve function");
    }
    if (input.getResultType() != ResolvedElementType.REFERENCE) {
      throw new InvalidRequestException(
          "Input to resolve function must be a Reference: " + input.getFhirPathExpression() + " ("
              + input.getResultTypeCode() + ")");
    }
    ResolvedElement element = resolveElement(input.getFhirPathExpression());
    assert element.getType() == ResolvedElementType.REFERENCE;
    String referenceTypeCode;
    if (element.getReferenceTypes().size() > 1) {
      if (arguments.size() == 1) {
        String argument = arguments.get(0).getFhirPathExpression();
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
                .getFhirPathExpression());
      }
    } else {
      String referenceTypeUrl = element.getReferenceTypes().get(0);
      StructureDefinition referenceTypeDefinition = getResourceByUrl(referenceTypeUrl);
      assert referenceTypeDefinition != null;
      referenceTypeCode = referenceTypeDefinition.getType();
    }
    // If there is a previous join and it is a lateral view, we need to convert it to an inline
    // query. Spark SQL does not support the direct chaining of a lateral view with a subsequent
    // table join.
    if (!input.getJoins().isEmpty()
        && input.getJoins().last().getJoinType() == JoinType.LATERAL_VIEW) {
      Join transformedJoin = transformLateralViewToInlineQuery(input.getJoins().last());
      input.getJoins().remove(input.getJoins().last());
      input.getJoins().add(transformedJoin);
    }
    List<String> pathComponents = Strings.tokenizePath(element.getPath());
    String joinAlias = Strings.pathToLowerSnakeCase(pathComponents);
    String joinExpression = "INNER JOIN " + referenceTypeCode.toLowerCase() + " " + joinAlias
        + " ON " + input.getSqlExpression() + ".reference = "
        + joinAlias + ".id";
    Join join = new Join(joinExpression, referenceTypeCode.toLowerCase(), JoinType.TABLE_JOIN,
        joinAlias);
    input.setResultType(ResolvedElementType.RESOURCE);
    input.setResultTypeCode(referenceTypeCode);
    input.setFhirPathExpression(referenceTypeCode);
    input.setSqlExpression(joinAlias);
    input.getJoins().add(join);
    return input;
  }

  private Join transformLateralViewToInlineQuery(Join last) {
    return null;
  }

}
