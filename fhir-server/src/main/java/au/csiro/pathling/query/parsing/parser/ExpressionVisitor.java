/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.parsing.parser;

import au.csiro.pathling.fhir.FhirPathBaseVisitor;
import au.csiro.pathling.fhir.FhirPathParser.*;
import au.csiro.pathling.query.operators.BinaryOperator;
import au.csiro.pathling.query.operators.BinaryOperatorInput;
import au.csiro.pathling.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;

/**
 * This class processes all types of expressions, and delegates the special handling of supported
 * types to the more specific visitor classes.
 *
 * @author John Grimes
 */
class ExpressionVisitor extends FhirPathBaseVisitor<ParsedExpression> {

  final ExpressionParserContext context;

  ExpressionVisitor(ExpressionParserContext context) {
    this.context = context;
  }

  /**
   * A term is typically a standalone literal or function invocation.
   */
  @Override
  public ParsedExpression visitTermExpression(TermExpressionContext ctx) {
    return ctx.term().accept(new ExpressionTermVisitor(context));
  }

  /**
   * An invocation expression is one expression invoking another using the dot notation.
   */
  @Override
  public ParsedExpression visitInvocationExpression(InvocationExpressionContext ctx) {
    ParsedExpression expressionResult = new ExpressionVisitor(context)
        .visit(ctx.expression());
    // The invoking expression is passed through to the invocation visitor's constructor - this
    // will provide it with extra context required to do things like merging in joins from the
    // upstream path.
    ParsedExpression invocationResult = ctx.invocation()
        .accept(new ExpressionInvocationVisitor(context, expressionResult));
    invocationResult
        .setFhirPath(expressionResult.getFhirPath() + "." + invocationResult.getFhirPath());
    return invocationResult;
  }

  @Nonnull
  private ParsedExpression visitBinaryOperator(ExpressionContext leftExpression,
      ExpressionContext rightExpression,
      String operatorString) {
    // Parse the left and right expressions.
    ParsedExpression leftResult = new ExpressionVisitor(context)
        .visit(leftExpression);
    ParsedExpression rightResult = new ExpressionVisitor(context)
        .visit(rightExpression);
    String fhirPath =
        leftResult.getFhirPath() + " " + operatorString + " " + rightResult.getFhirPath();

    // Retrieve a BinaryOperator object based upon the operator.
    BinaryOperator operator = BinaryOperator.getBinaryOperator(operatorString);

    // Collect the input information.
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(leftResult);
    input.setRight(rightResult);
    input.setExpression(fhirPath);
    input.setContext(context);

    // Return the result.
    return operator.invoke(input);
  }

  @Override
  public ParsedExpression visitEqualityExpression(EqualityExpressionContext ctx) {
    String operatorString = ctx.children.get(1).toString();
    if (operatorString.equals("~")) {
      throw new InvalidRequestException("Equivalent operator (~) is not supported");
    }
    if (operatorString.equals("!~")) {
      throw new InvalidRequestException("Not equivalent operator (!~) is not supported");
    }
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        operatorString);
  }

  @Override
  public ParsedExpression visitInequalityExpression(InequalityExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public ParsedExpression visitAndExpression(AndExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public ParsedExpression visitOrExpression(OrExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public ParsedExpression visitImpliesExpression(ImpliesExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public ParsedExpression visitMembershipExpression(MembershipExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public ParsedExpression visitMultiplicativeExpression(MultiplicativeExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public ParsedExpression visitAdditiveExpression(AdditiveExpressionContext ctx) {
    String operatorString = ctx.children.get(1).toString();
    if (operatorString.equals("&")) {
      throw new InvalidRequestException("String concatenation operator (&) is not supported");
    }
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        operatorString);
  }

  // All other FHIRPath constructs are currently unsupported.

  @Override
  public ParsedExpression visitIndexerExpression(IndexerExpressionContext ctx) {
    throw new InvalidRequestException("Indexer operation is not supported");
  }

  @Override
  public ParsedExpression visitPolarityExpression(PolarityExpressionContext ctx) {
    throw new InvalidRequestException("Polarity operator is not supported");
  }

  @Override
  public ParsedExpression visitUnionExpression(UnionExpressionContext ctx) {
    throw new InvalidRequestException("Union expressions are not supported");
  }

  @Override
  public ParsedExpression visitTypeExpression(TypeExpressionContext ctx) {
    throw new InvalidRequestException("Type expressions are not supported");
  }

}
