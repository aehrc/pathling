/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.fhir.ResourceDefinitions.getBaseResource;
import static au.csiro.clinsight.fhir.ResourceDefinitions.getElementDefinition;

import au.csiro.clinsight.fhir.FhirPathBaseVisitor;
import au.csiro.clinsight.fhir.FhirPathParser.FunctionInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.InvocationExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.MemberInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.TermExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.ThisInvocationContext;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class knows how to parse an invocation expression (e.g. Patient.name.firstName), while also
 * validating that the path is valid according to a known FHIR resource definition.
 *
 * @author John Grimes
 */
class ValidatingInvocationParser extends FhirPathBaseVisitor<ParseResult> {

  private static final Logger logger = LoggerFactory.getLogger(ValidatingInvocationParser.class);

  private static void validateResourceIdentifier(String resourceIdentifier) {
    if (getBaseResource(resourceIdentifier) == null) {
      throw new InvalidRequestException("Resource identifier not known: " + resourceIdentifier);
    }
  }

  private static void validateElement(String invocationExpression) {
    String[] pathComponents = invocationExpression.split("\\.");
    pathComponents[0] = Strings.capitalize(pathComponents[0]);
    String path = String.join(".", pathComponents);
    if (getElementDefinition(path) == null) {
      throw new InvalidRequestException("Element not known: " + path);
    }
  }

  @Override
  public ParseResult visitInvocationExpression(InvocationExpressionContext ctx) {
    logger.debug("Invocation expression: " + ctx.getText());
    ParseResult expressionResult = ctx.expression().accept(new ValidatingInvocationParser());
    ParseResult invocationResult = ctx.invocation().accept(new ValidatingInvocationParser());
    String expression = expressionResult.getExpression() + "." + invocationResult.getExpression();
    validateElement(expression);
    ParseResult result = new ParseResult(expression);
    result.setFromTable(expressionResult.getFromTable());
    return result;
  }

  @Override
  public ParseResult visitTermExpression(TermExpressionContext ctx) {
    logger.debug("Term expression: " + ctx.getText());
    String resourceIdentifier = ctx.getText();
    validateResourceIdentifier(resourceIdentifier);
    ParseResult result = new ParseResult(resourceIdentifier.toLowerCase());
    result.setFromTable(result.getExpression());
    return result;
  }

  @Override
  public ParseResult visitMemberInvocation(MemberInvocationContext ctx) {
    logger.debug("Member invocation: " + ctx.getText());
    return new ParseResult(ctx.getText());
  }

  @Override
  public ParseResult visitFunctionInvocation(FunctionInvocationContext ctx) {
    logger.debug("Function invocation: " + ctx.getText());
    return new ParseResult(ctx.getText());
  }

  @Override
  public ParseResult visitThisInvocation(ThisInvocationContext ctx) {
    logger.debug("$this invocation: " + ctx.getText());
    return new ParseResult(ctx.getText());
  }

}
