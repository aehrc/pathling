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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.expression.TypeSpecifier;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.AdditiveExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.AndExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.BooleanLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.CodingLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.CombineExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.DateLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.DateTimeLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.DateTimePrecisionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.EqualityExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ExternalConstantContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ExternalConstantTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.FunctionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.FunctionInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.IdentifierContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ImpliesExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.IndexInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.IndexerExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.InequalityExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.InvocationExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.InvocationTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.LiteralTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.MemberInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.MembershipExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.MultiplicativeExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.NullLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.NumberLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.OrExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ParamListContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ParenthesizedTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.PluralDateTimePrecisionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.PolarityExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.QualifiedIdentifierContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.QuantityContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.QuantityLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.StringLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TermExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ThisInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TimeLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TotalInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TypeExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TypeSpecifierContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.UnionExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.UnitContext;
import jakarta.annotation.Nonnull;

/**
 * A special visitor for the type specifiers arguments in the FHIRPath function invocations.
 *
 * @author Piotr Szul
 */
class TypeSpecifierVisitor extends FhirPathBaseVisitor<FhirPath> {

  private final boolean isNamespace;

  private TypeSpecifierVisitor(final boolean isNamespace) {
    this.isNamespace = isNamespace;
  }

  TypeSpecifierVisitor() {
    this(false);
  }

  @Override
  public FhirPath visitIdentifier(final IdentifierContext ctx) {
    return new TypeSpecifier(ctx.getText());
  }

  @Override
  public FhirPath visitInvocationExpression(final InvocationExpressionContext ctx) {
    // If we are not already in a namespace and there is an invocation, we need to parse the 
    // right-hand side of the invocation within the namespace.
    if (!isNamespace) {
      final TypeSpecifier typeSpecifier = (TypeSpecifier) ctx.expression()
          .accept(new TypeSpecifierVisitor(true));
      return typeSpecifier.withNamespace(ctx.invocation().getText());
    } else {
      throw unexpected("InvocationExpression");
    }
  }

  @Override
  public FhirPath visitTermExpression(final TermExpressionContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public FhirPath visitInvocationTerm(final InvocationTermContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public FhirPath visitMemberInvocation(final MemberInvocationContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public FhirPath visitIndexerExpression(
      final IndexerExpressionContext ctx) {
    throw unexpected("IndexerExpression");
  }

  @Override
  public FhirPath visitPolarityExpression(
      final PolarityExpressionContext ctx) {
    throw unexpected("PolarityExpression");
  }

  @Override
  public FhirPath visitAdditiveExpression(
      final AdditiveExpressionContext ctx) {
    throw unexpected("AdditiveExpression");
  }

  @Override
  public FhirPath visitCombineExpression(
      final CombineExpressionContext ctx) {
    throw unexpected("CombineExpression");
  }

  @Override
  public FhirPath visitMultiplicativeExpression(
      final MultiplicativeExpressionContext ctx) {
    throw unexpected("MultiplicativeExpression");
  }

  @Override
  public FhirPath visitUnionExpression(final UnionExpressionContext ctx) {
    throw unexpected("UnionExpression");
  }

  @Override
  public FhirPath visitOrExpression(final OrExpressionContext ctx) {
    throw unexpected("OrExpression");
  }

  @Override
  public FhirPath visitAndExpression(final AndExpressionContext ctx) {
    throw unexpected("AndExpression");
  }

  @Override
  public FhirPath visitMembershipExpression(
      final MembershipExpressionContext ctx) {
    throw unexpected("MembershipExpression");
  }

  @Override
  public FhirPath visitInequalityExpression(
      final InequalityExpressionContext ctx) {
    throw unexpected("InequalityExpression");
  }

  @Override
  public FhirPath visitEqualityExpression(
      final EqualityExpressionContext ctx) {
    throw unexpected("EqualityExpression");
  }

  @Override
  public FhirPath visitImpliesExpression(
      final ImpliesExpressionContext ctx) {
    throw unexpected("ImpliesExpression");
  }

  @Override
  public FhirPath visitTypeExpression(final TypeExpressionContext ctx) {
    throw unexpected("TypeExpression");
  }

  @Override
  public FhirPath visitLiteralTerm(final LiteralTermContext ctx) {
    throw unexpected("LiteralTerm");
  }

  @Override
  public FhirPath visitExternalConstantTerm(
      final ExternalConstantTermContext ctx) {
    throw unexpected("ExternalConstantTerm");
  }

  @Override
  public FhirPath visitParenthesizedTerm(
      final ParenthesizedTermContext ctx) {
    throw unexpected("ParenthesizedTerm");
  }

  @Override
  public FhirPath visitNullLiteral(final NullLiteralContext ctx) {
    throw unexpected("NullLiteral");
  }

  @Override
  public FhirPath visitBooleanLiteral(final BooleanLiteralContext ctx) {
    throw unexpected("BooleanLiteral");
  }

  @Override
  public FhirPath visitStringLiteral(final StringLiteralContext ctx) {
    throw unexpected("StringLiteral");
  }

  @Override
  public FhirPath visitNumberLiteral(final NumberLiteralContext ctx) {
    throw unexpected("NumberLiteral");
  }

  @Override
  public FhirPath visitDateLiteral(final DateLiteralContext ctx) {
    throw unexpected("DateLiteral");
  }

  @Override
  public FhirPath visitDateTimeLiteral(final DateTimeLiteralContext ctx) {
    throw unexpected("DateTimeLiteral");
  }

  @Override
  public FhirPath visitTimeLiteral(final TimeLiteralContext ctx) {
    throw unexpected("TimeLiteral");
  }

  @Override
  public FhirPath visitQuantityLiteral(final QuantityLiteralContext ctx) {
    throw unexpected("QuantityLiteral");
  }

  @Override
  public FhirPath visitCodingLiteral(final CodingLiteralContext ctx) {
    throw unexpected("CodingLiteral");
  }

  @Override
  public FhirPath visitExternalConstant(final ExternalConstantContext ctx) {
    throw unexpected("ExternalConstant");
  }

  @Override
  public FhirPath visitFunctionInvocation(
      final FunctionInvocationContext ctx) {
    throw unexpected("FunctionInvocation");
  }

  @Override
  public FhirPath visitThisInvocation(final ThisInvocationContext ctx) {
    throw unexpected("ThisInvocation");
  }

  @Override
  public FhirPath visitIndexInvocation(final IndexInvocationContext ctx) {
    throw unexpected("IndexInvocation");
  }

  @Override
  public FhirPath visitTotalInvocation(final TotalInvocationContext ctx) {
    throw unexpected("TotalInvocation");
  }

  @Override
  public FhirPath visitFunction(final FunctionContext ctx) {
    throw unexpected("Function");
  }

  @Override
  public FhirPath visitParamList(final ParamListContext ctx) {
    throw unexpected("ParamList");
  }

  @Override
  public FhirPath visitQuantity(final QuantityContext ctx) {
    throw unexpected("Quantity");
  }

  @Override
  public FhirPath visitUnit(final UnitContext ctx) {
    throw unexpected("Unit");
  }

  @Override
  public FhirPath visitDateTimePrecision(
      final DateTimePrecisionContext ctx) {
    throw unexpected("DateTimePrecision");
  }

  @Override
  public FhirPath visitPluralDateTimePrecision(
      final PluralDateTimePrecisionContext ctx) {
    throw unexpected("PluralDateTimePrecision");
  }

  @Override
  public FhirPath visitTypeSpecifier(final TypeSpecifierContext ctx) {
    throw unexpected("TypeSpecifier");
  }

  @Override
  public FhirPath visitQualifiedIdentifier(
      final QualifiedIdentifierContext ctx) {
    throw unexpected("QualifiedIdentifier");
  }

  @Nonnull
  private static RuntimeException unexpected(final String expressionType) {
    return new UnsupportedExpressionException("Unexpected expression type: " + expressionType);
  }

}
