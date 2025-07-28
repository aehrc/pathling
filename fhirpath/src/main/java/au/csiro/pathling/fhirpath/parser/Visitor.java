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

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.operator.BinaryOperator;
import au.csiro.pathling.fhirpath.operator.BinaryOperatorType;
import au.csiro.pathling.fhirpath.operator.CollectionOperations;
import au.csiro.pathling.fhirpath.operator.MethodDefinedOperator;
import au.csiro.pathling.fhirpath.operator.MethodInvocationError;
import au.csiro.pathling.fhirpath.operator.PolarityOperator;
import au.csiro.pathling.fhirpath.operator.SubsettingOperations;
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
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalOperator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * This class processes all types of expressions, and delegates the special handling of supported
 * types to the more specific visitor classes.
 *
 * @author John Grimes
 */
class Visitor extends FhirPathBaseVisitor<FhirPath> {

  /**
   * A term is typically a standalone literal or function invocation.
   *
   * @param ctx The {@link TermExpressionContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  @Nonnull
  public FhirPath visitTermExpression(
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
  public FhirPath visitInvocationExpression(
      @Nullable final InvocationExpressionContext ctx) {

    final FhirPath invocationSubject = new Visitor().visit(
        requireNonNull(ctx).expression());
    final FhirPath invocationVerb = ctx.invocation()
        .accept(new InvocationVisitor());
    return invocationSubject.andThen(invocationVerb);
  }

  private static final Map<String, BinaryOperator> BINARY_OPERATORS = MethodDefinedOperator.mapOf(
      CollectionOperations.class);

  @Nonnull
  private FhirPath visitBinaryOperator(
      @Nullable final ParseTree leftContext,
      @Nullable final ParseTree rightContext, @Nullable final String operatorName) {
    requireNonNull(operatorName);
    return new EvalOperator(new Visitor().visit(leftContext),
        new Visitor().visit(rightContext),
        Optional.ofNullable(BINARY_OPERATORS.get(operatorName))
            .orElseGet(() -> BinaryOperatorType.fromSymbol(operatorName).getInstance()));

  }

  @Override
  @Nonnull
  public FhirPath visitEqualityExpression(
      @Nullable final EqualityExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitInequalityExpression(
      @Nullable final InequalityExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitAndExpression(
      @Nullable final AndExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitOrExpression(
      @Nullable final OrExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitImpliesExpression(
      @Nullable final ImpliesExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitMembershipExpression(
      @Nullable final MembershipExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitMultiplicativeExpression(
      @Nullable final MultiplicativeExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitAdditiveExpression(
      @Nullable final AdditiveExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitCombineExpression(
      @Nullable final CombineExpressionContext ctx) {
    return visitBinaryOperator(requireNonNull(ctx).expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  @Nonnull
  public FhirPath visitIndexerExpression(
      final IndexerExpressionContext ctx) {
    final EvalOperator operator;
    try {
      operator = new EvalOperator(
          new Visitor().visit(requireNonNull(ctx).expression(0)),
          new Visitor().visit(ctx.expression(1)),
          // Get a wrapped version of the index operator.
          MethodDefinedOperator.build(
              SubsettingOperations.class.getDeclaredMethod("index", Collection.class,
                  IntegerCollection.class)));
    } catch (final NoSuchMethodException e) {
      throw new MethodInvocationError("Problem invoking the index operator", e);
    }
    return operator;
  }

  // All other FHIRPath constructs are currently unsupported.

  @Override
  @Nonnull
  public FhirPath visitPolarityExpression(
      final PolarityExpressionContext ctx) {
    return new Paths.EvalUnaryOperator(
        ctx.expression().accept(this),
        PolarityOperator.fromSymbol(ctx.children.get(0).toString())
    );
  }

  @Override
  @Nonnull
  public FhirPath visitUnionExpression(final UnionExpressionContext ctx) {
    throw new UnsupportedFhirPathFeatureError("Union expressions are not supported");
  }

  @Override
  @Nonnull
  public FhirPath visitTypeExpression(final TypeExpressionContext ctx) {
    throw new UnsupportedFhirPathFeatureError("Type expressions are not supported");
  }

}
