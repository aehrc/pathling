/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.FhirPathBaseVisitor;
import au.csiro.pathling.fhir.FhirPathParser.*;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.operator.Operator;
import au.csiro.pathling.fhirpath.operator.OperatorInput;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * This class processes all types of expressions, and delegates the special handling of supported
 * types to the more specific visitor classes.
 *
 * @author John Grimes
 */
class Visitor extends FhirPathBaseVisitor<ParserResult> {

  @Nonnull
  private final ParserContext context;

  Visitor(@Nonnull final ParserContext context) {
    this.context = context;
  }

  /**
   * A term is typically a standalone literal or function invocation.
   *
   * @param ctx The {@link TermExpressionContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  @Nonnull
  public ParserResult visitTermExpression(@Nonnull final TermExpressionContext ctx) {
    return ctx.term().accept(new TermVisitor(context));
  }

  /**
   * An invocation expression is one expression invoking another using the dot notation.
   *
   * @param ctx The {@link InvocationExpressionContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  @Nonnull
  public ParserResult visitInvocationExpression(@Nonnull final InvocationExpressionContext ctx) {
    final ParserResult expressionResult = new Visitor(context).visit(ctx.expression());
    return ctx.invocation()
        .accept(new InvocationVisitor(expressionResult.getEvaluationContext(),
            expressionResult.getFhirPath()));
  }

  @Nonnull
  private ParserResult visitBinaryOperator(@Nullable final ParseTree leftContext,
      @Nullable final ParseTree rightContext, @Nullable final String operatorName) {
    checkNotNull(operatorName);

    // Parse the left and right expressions.
    final ParserResult left = new Visitor(context).visit(leftContext);
    final ParserResult right = new Visitor(context).visit(rightContext);

    // Retrieve an Operator instance based upon the operator string.
    final Operator operator = Operator.getInstance(operatorName);

    final OperatorInput operatorInput = new OperatorInput(context, left.getFhirPath(),
        right.getFhirPath());
    return context.resultFor(operator.invoke(operatorInput));
  }

  @Override
  @Nonnull
  public ParserResult visitEqualityExpression(@Nonnull final EqualityExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public ParserResult visitInequalityExpression(@Nonnull final InequalityExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public ParserResult visitAndExpression(@Nonnull final AndExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public ParserResult visitOrExpression(@Nonnull final OrExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public ParserResult visitImpliesExpression(@Nonnull final ImpliesExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public ParserResult visitMembershipExpression(@Nonnull final MembershipExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public ParserResult visitMultiplicativeExpression(
      @Nonnull final MultiplicativeExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public ParserResult visitAdditiveExpression(@Nonnull final AdditiveExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  // All other FHIRPath constructs are currently unsupported.

  @Override
  @Nonnull
  public ParserResult visitIndexerExpression(final IndexerExpressionContext ctx) {
    throw new InvalidUserInputError("Indexer operation is not supported");
  }

  @Override
  @Nonnull
  public ParserResult visitPolarityExpression(final PolarityExpressionContext ctx) {
    throw new InvalidUserInputError("Polarity operator is not supported");
  }

  @Override
  @Nonnull
  public ParserResult visitUnionExpression(final UnionExpressionContext ctx) {
    throw new InvalidUserInputError("Union expressions are not supported");
  }

  @Override
  @Nonnull
  public ParserResult visitTypeExpression(final TypeExpressionContext ctx) {
    throw new InvalidUserInputError("Type expressions are not supported");
  }

}
