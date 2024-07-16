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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.expression.BinaryOperatorCall;
import au.csiro.pathling.fhirpath.expression.Invocation;
import au.csiro.pathling.fhirpath.expression.TypeSpecifier;
import au.csiro.pathling.fhirpath.expression.UnaryOperatorCall;
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
  public FhirPath visitTermExpression(final TermExpressionContext ctx) {
    return requireNonNull(ctx).term().accept(new TermVisitor());
  }

  /**
   * An invocation expression is one expression invoking another using the dot notation.
   *
   * @param ctx The {@link InvocationExpressionContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  public FhirPath visitInvocationExpression(final InvocationExpressionContext ctx) {
    final FhirPath source = new Visitor().visit(ctx.expression());
    final FhirPath target = ctx.invocation().accept(new InvocationVisitor(source));
    return new Invocation(source, target);
  }

  private FhirPath visitBinaryOperator(final ParseTree leftContext, final ParseTree rightContext,
      final String operatorName) {
    return new BinaryOperatorCall(new Visitor().visit(leftContext),
        new Visitor().visit(rightContext), operatorName);
  }

  @Override
  public FhirPath visitEqualityExpression(final EqualityExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitInequalityExpression(final InequalityExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitAndExpression(final AndExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitOrExpression(final OrExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitImpliesExpression(final ImpliesExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitMembershipExpression(final MembershipExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitMultiplicativeExpression(final MultiplicativeExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitAdditiveExpression(final AdditiveExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitCombineExpression(final CombineExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitIndexerExpression(final IndexerExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1), "index");
  }

  @Override
  public FhirPath visitPolarityExpression(final PolarityExpressionContext ctx) {
    return new UnaryOperatorCall(new Visitor().visit(ctx.expression()),
        ctx.children.get(0).toString());
  }

  @Override
  public FhirPath visitUnionExpression(final UnionExpressionContext ctx) {
    return visitBinaryOperator(ctx.expression(0), ctx.expression(1),
        ctx.children.get(1).toString());
  }

  @Override
  public FhirPath visitTypeExpression(final TypeExpressionContext ctx) {
    final TypeSpecifier typeSpecifier = new TypeSpecifierVisitor().visit(ctx.typeSpecifier());
    final String operatorName = ctx.children.get(1).toString();
    return new BinaryOperatorCall(new Visitor().visit(ctx.expression()),
        typeSpecifier, operatorName);
  }

}
