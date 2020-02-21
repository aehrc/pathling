/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.parsing.parser;

import au.csiro.pathling.fhir.FhirPathBaseVisitor;
import au.csiro.pathling.fhir.FhirPathParser.ExternalConstantTermContext;
import au.csiro.pathling.fhir.FhirPathParser.InvocationTermContext;
import au.csiro.pathling.fhir.FhirPathParser.LiteralTermContext;
import au.csiro.pathling.fhir.FhirPathParser.ParenthesizedTermContext;
import au.csiro.pathling.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;

/**
 * A term is typically a standalone literal or function invocation.
 *
 * @author John Grimes
 */
class ExpressionTermVisitor extends FhirPathBaseVisitor<ParsedExpression> {

  final ExpressionParserContext context;

  ExpressionTermVisitor(ExpressionParserContext context) {
    this.context = context;
  }

  /**
   * This passes a standalone function invocation along to the invocation visitor. Note that most
   * functions will require an input, and will fail validation later on in this instance.
   */
  @Override
  public ParsedExpression visitInvocationTerm(InvocationTermContext ctx) {
    return new ExpressionInvocationVisitor(context).visit(ctx.invocation());
  }

  /**
   * We pass literals as is through to the literal visitor.
   */
  @Override
  public ParsedExpression visitLiteralTerm(LiteralTermContext ctx) {
    return new ExpressionLiteralTermVisitor(context).visit(ctx.literal());
  }

  @Override
  public ParsedExpression visitExternalConstantTerm(ExternalConstantTermContext ctx) {
    if (ctx.getText().equals("%resource") || ctx.getText().equals("%context")) {
      ParsedExpression result = context.getSubjectContext();
      result.setFhirPath(ctx.getText());
      result.setOrigin(result);
      return result;
    } else {
      throw new InvalidRequestException("Unrecognised environment variable: " + ctx.getText());
    }
  }

  /**
   * Parentheses are ignored in the standalone term case.
   */
  @Override
  public ParsedExpression visitParenthesizedTerm(ParenthesizedTermContext ctx) {
    final ParsedExpression result = new ExpressionVisitor(context)
        .visit(ctx.expression());
    result.setFhirPath(ctx.getText());
    return result;
  }

}
