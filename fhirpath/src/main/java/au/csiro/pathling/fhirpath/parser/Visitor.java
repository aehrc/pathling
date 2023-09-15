/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.parser;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.operator.BinaryOperator;
import au.csiro.pathling.fhirpath.operator.BinaryOperatorType;
import au.csiro.pathling.fhirpath.operator.BinaryOperators;
import au.csiro.pathling.fhirpath.operator.WrappedBinaryOperator;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.AdditiveExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.AndExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.CombineExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.EqualityExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ImpliesExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.IndexerExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.InequalityExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.InvocationExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.MembershipExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.MultiplicativeExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.OrExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.PolarityExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TermExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TypeExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.UnionExpressionContext;
import au.csiro.pathling.fhirpath.path.EvalOperatorPath;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.antlr.v4.runtime.tree.ParseTree;
import java.util.Map;

/**
 * This class processes all types of expressions, and delegates the special handling of supported
 * types to the more specific visitor classes.
 *
 * @author John Grimes
 */
class Visitor extends FhirPathBaseVisitor<FhirPath<Collection, Collection>> {

  /**
   * A term is typically a standalone literal or function invocation.
   *
   * @param ctx The {@link TermExpressionContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitTermExpression(
      @Nullable final TermExpressionContext ctx) {
    return requireNonNull(ctx).term().accept(new TermVisitor());
  }

  /**
   * An invocation expression is one expression invoking another using the dot notation.
   *
   * @param ctx The {@link InvocationExpressionContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitInvocationExpression(
      @Nullable final InvocationExpressionContext ctx) {

    // TODO: Is this really OK (now I am a bit confused of what the context is vs input)

    final FhirPath<Collection, Collection> invocationSubject = new Visitor().visit(
        requireNonNull(ctx).expression());
    final FhirPath<Collection, Collection> invocationVerb = ctx.invocation()
        .accept(new InvocationVisitor());
    return (input, context) -> {
      // TODO: perhpas we should also create the new cotext here (with different %context)
      return invocationVerb.apply(invocationSubject.apply(input, context), context);
    };
  }


  private static final Map<String, BinaryOperator> BINARY_OPERATORS = WrappedBinaryOperator.mapOf(
      BinaryOperators.class);

  @Nonnull
  private FhirPath<Collection, Collection> visitBinaryOperator(
      @Nullable final ParseTree leftContext,
      @Nullable final ParseTree rightContext, @Nullable final String operatorName) {
    requireNonNull(operatorName);
    return new EvalOperatorPath(new Visitor().visit(leftContext),
        new Visitor().visit(rightContext),
        BINARY_OPERATORS.getOrDefault(operatorName,
            BinaryOperatorType.fromSymbol(operatorName).getInstance()));

  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitEqualityExpression(
      @Nullable final EqualityExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath<Collection, Collection> visitInequalityExpression(
      @Nullable final InequalityExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitAndExpression(
      @Nullable final AndExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitOrExpression(
      @Nullable final OrExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitImpliesExpression(
      @Nullable final ImpliesExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitMembershipExpression(
      @Nullable final MembershipExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitMultiplicativeExpression(
      @Nullable final MultiplicativeExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitAdditiveExpression(
      @Nullable final AdditiveExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath<Collection, Collection> visitCombineExpression(
      @Nullable final CombineExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  // All other FHIRPath constructs are currently unsupported.

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitIndexerExpression(
      final IndexerExpressionContext ctx) {
    throw new InvalidUserInputError("Indexer operation is not supported");
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitPolarityExpression(
      final PolarityExpressionContext ctx) {
    throw new InvalidUserInputError("Polarity operator is not supported");
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitUnionExpression(final UnionExpressionContext ctx) {
    throw new InvalidUserInputError("Union expressions are not supported");
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitTypeExpression(final TypeExpressionContext ctx) {
    throw new InvalidUserInputError("Type expressions are not supported");
  }

}
