/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.fhir.ResourceDefinitions.getBaseResource;

import au.csiro.clinsight.fhir.FhirPathBaseVisitor;
import au.csiro.clinsight.fhir.FhirPathParser.FunctionInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.InvocationExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.MemberInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.TermExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.ThisInvocationContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;

/**
 * This class knows how to parse an invocation expression (e.g. Patient.name.firstName), while also
 * validating that the path is valid according to a known FHIR resource definition.
 *
 * @author John Grimes
 */
class ValidatingInvocationParser extends FhirPathBaseVisitor<ParseResult> {

  private static void validateResourceIdentifier(String resourceIdentifier) {
    if (getBaseResource(resourceIdentifier) == null) {
      throw new InvalidRequestException("Resource identifier not known: " + resourceIdentifier);
    }
  }

  @Override
  public ParseResult visitInvocationExpression(InvocationExpressionContext ctx) {
    ParseResult expressionResult = ctx.expression().accept(new ValidatingInvocationParser());
    ParseResult invocationResult = ctx.invocation().accept(new ValidatingInvocationParser());
    String expression = expressionResult.getExpression() + "." + invocationResult.getExpression();
    ParseResult result = new ParseResult(expression);
    result.setFromTable(expressionResult.getFromTable());
    return result;
  }

  @Override
  public ParseResult visitTermExpression(TermExpressionContext ctx) {
    String resourceIdentifier = ctx.getText();
    validateResourceIdentifier(resourceIdentifier);
    ParseResult result = new ParseResult(resourceIdentifier.toLowerCase());
    result.setFromTable(result.getExpression());
    return result;
  }

  @Override
  public ParseResult visitMemberInvocation(MemberInvocationContext ctx) {
    return new ParseResult(ctx.getText());
  }

  @Override
  public ParseResult visitFunctionInvocation(FunctionInvocationContext ctx) {
    return new ParseResult(ctx.getText());
  }

  @Override
  public ParseResult visitThisInvocation(ThisInvocationContext ctx) {
    return new ParseResult(ctx.getText());
  }

}
