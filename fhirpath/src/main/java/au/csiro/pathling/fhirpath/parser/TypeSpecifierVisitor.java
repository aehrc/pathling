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

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
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
import au.csiro.pathling.fhirpath.path.Paths;

import javax.annotation.Nonnull;

/**
 * A special visitor for the type specifiers arguments in the FHIRPath function invocations.
 *
 * @author Piotr Szul
 */
class TypeSpecifierVisitor extends FhirPathBaseVisitor<FhirPath<Collection>> {

  private final boolean isNamespace;

  TypeSpecifierVisitor(final boolean isNamespace) {
    this.isNamespace = isNamespace;
  }

  TypeSpecifierVisitor() {
    this(false);
  }

  @Override
  public FhirPath<Collection> visitIndexerExpression(
      final IndexerExpressionContext ctx) {
    throw newUnexpectedExpressionException("IndexerExpression");
  }

  @Override
  public FhirPath<Collection> visitPolarityExpression(
      final PolarityExpressionContext ctx) {
    throw newUnexpectedExpressionException("PolarityExpression");
  }

  @Override
  public FhirPath<Collection> visitAdditiveExpression(
      final AdditiveExpressionContext ctx) {
    throw newUnexpectedExpressionException("AdditiveExpression");
  }

  @Override
  public FhirPath<Collection> visitCombineExpression(
      final CombineExpressionContext ctx) {
    throw newUnexpectedExpressionException("CombineExpression");
  }

  @Override
  public FhirPath<Collection> visitMultiplicativeExpression(
      final MultiplicativeExpressionContext ctx) {
    throw newUnexpectedExpressionException("MultiplicativeExpression");
  }

  @Override
  public FhirPath<Collection> visitUnionExpression(final UnionExpressionContext ctx) {
    throw newUnexpectedExpressionException("UnionExpression");
  }

  @Override
  public FhirPath<Collection> visitOrExpression(final OrExpressionContext ctx) {
    throw newUnexpectedExpressionException("OrExpression");
  }

  @Override
  public FhirPath<Collection> visitAndExpression(final AndExpressionContext ctx) {
    throw newUnexpectedExpressionException("AndExpression");
  }

  @Override
  public FhirPath<Collection> visitMembershipExpression(
      final MembershipExpressionContext ctx) {
    throw newUnexpectedExpressionException("MembershipExpression");
  }

  @Override
  public FhirPath<Collection> visitInequalityExpression(
      final InequalityExpressionContext ctx) {
    throw newUnexpectedExpressionException("InequalityExpression");
  }

  @Override
  public FhirPath<Collection> visitInvocationExpression(
      final InvocationExpressionContext ctx) {
    if (!isNamespace) {
      final TypeSpecifier unqualifiedTypeSpecifier = ((Paths.TypeSpecifierPath) ctx.expression()
          .accept(new TypeSpecifierVisitor(true))).getTypeSpecifier();
      return new Paths.TypeSpecifierPath(
          unqualifiedTypeSpecifier.withNamespace(ctx.invocation().getText()));
    } else {
      throw newUnexpectedExpressionException("InvocationExpression");
    }
  }

  @Override
  public FhirPath<Collection> visitEqualityExpression(
      final EqualityExpressionContext ctx) {
    throw newUnexpectedExpressionException("EqualityExpression");
  }

  @Override
  public FhirPath<Collection> visitImpliesExpression(
      final ImpliesExpressionContext ctx) {
    throw newUnexpectedExpressionException("ImpliesExpression");
  }

  @SuppressWarnings("RedundantMethodOverride")
  @Override
  public FhirPath<Collection> visitTermExpression(final TermExpressionContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public FhirPath<Collection> visitTypeExpression(final TypeExpressionContext ctx) {
    throw newUnexpectedExpressionException("TypeExpression");
  }

  @SuppressWarnings("RedundantMethodOverride")
  @Override
  public FhirPath<Collection> visitInvocationTerm(final InvocationTermContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public FhirPath<Collection> visitLiteralTerm(final LiteralTermContext ctx) {
    throw newUnexpectedExpressionException("LiteralTerm");
  }

  @Override
  public FhirPath<Collection> visitExternalConstantTerm(
      final ExternalConstantTermContext ctx) {
    throw newUnexpectedExpressionException("ExternalConstantTerm");
  }

  @Override
  public FhirPath<Collection> visitParenthesizedTerm(
      final ParenthesizedTermContext ctx) {
    throw newUnexpectedExpressionException("ParenthesizedTerm");
  }

  @Override
  public FhirPath<Collection> visitNullLiteral(final NullLiteralContext ctx) {
    throw newUnexpectedExpressionException("NullLiteral");
  }

  @Override
  public FhirPath<Collection> visitBooleanLiteral(final BooleanLiteralContext ctx) {
    throw newUnexpectedExpressionException("BooleanLiteral");
  }

  @Override
  public FhirPath<Collection> visitStringLiteral(final StringLiteralContext ctx) {
    throw newUnexpectedExpressionException("StringLiteral");
  }

  @Override
  public FhirPath<Collection> visitNumberLiteral(final NumberLiteralContext ctx) {
    throw newUnexpectedExpressionException("NumberLiteral");
  }

  @Override
  public FhirPath<Collection> visitDateLiteral(final DateLiteralContext ctx) {
    throw newUnexpectedExpressionException("DateLiteral");
  }

  @Override
  public FhirPath<Collection> visitDateTimeLiteral(final DateTimeLiteralContext ctx) {
    throw newUnexpectedExpressionException("DateTimeLiteral");
  }

  @Override
  public FhirPath<Collection> visitTimeLiteral(final TimeLiteralContext ctx) {
    throw newUnexpectedExpressionException("TimeLiteral");
  }

  @Override
  public FhirPath<Collection> visitQuantityLiteral(final QuantityLiteralContext ctx) {
    throw newUnexpectedExpressionException("QuantityLiteral");
  }

  @Override
  public FhirPath<Collection> visitCodingLiteral(final CodingLiteralContext ctx) {
    throw newUnexpectedExpressionException("CodingLiteral");
  }

  @Override
  public FhirPath<Collection> visitExternalConstant(final ExternalConstantContext ctx) {
    throw newUnexpectedExpressionException("ExternalConstant");
  }

  @SuppressWarnings("RedundantMethodOverride")
  @Override
  public FhirPath<Collection> visitMemberInvocation(final MemberInvocationContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public FhirPath<Collection> visitFunctionInvocation(
      final FunctionInvocationContext ctx) {
    throw newUnexpectedExpressionException("FunctionInvocation");
  }

  @Override
  public FhirPath<Collection> visitThisInvocation(final ThisInvocationContext ctx) {
    throw newUnexpectedExpressionException("ThisInvocation");
  }

  @Override
  public FhirPath<Collection> visitIndexInvocation(final IndexInvocationContext ctx) {
    throw newUnexpectedExpressionException("IndexInvocation");
  }

  @Override
  public FhirPath<Collection> visitTotalInvocation(final TotalInvocationContext ctx) {
    throw newUnexpectedExpressionException("TotalInvocation");
  }

  @Override
  public FhirPath<Collection> visitFunction(final FunctionContext ctx) {
    throw newUnexpectedExpressionException("Function");
  }

  @Override
  public FhirPath<Collection> visitParamList(final ParamListContext ctx) {
    throw newUnexpectedExpressionException("ParamList");
  }

  @Override
  public FhirPath<Collection> visitQuantity(final QuantityContext ctx) {
    throw newUnexpectedExpressionException("Quantity");
  }

  @Override
  public FhirPath<Collection> visitUnit(final UnitContext ctx) {
    throw newUnexpectedExpressionException("Unit");
  }

  @Override
  public FhirPath<Collection> visitDateTimePrecision(
      final DateTimePrecisionContext ctx) {
    throw newUnexpectedExpressionException("DateTimePrecision");
  }

  @Override
  public FhirPath<Collection> visitPluralDateTimePrecision(
      final PluralDateTimePrecisionContext ctx) {
    throw newUnexpectedExpressionException("PluralDateTimePrecision");
  }

  @Override
  public FhirPath<Collection> visitTypeSpecifier(final TypeSpecifierContext ctx) {
    throw newUnexpectedExpressionException("TypeSpecifier");
  }

  @Override
  public FhirPath<Collection> visitQualifiedIdentifier(
      final QualifiedIdentifierContext ctx) {
    throw newUnexpectedExpressionException("QualifiedIdentifier");
  }

  @Override
  public FhirPath<Collection> visitIdentifier(final IdentifierContext ctx) {
    return new Paths.TypeSpecifierPath(new TypeSpecifier(ctx.getText()));
  }

  @Nonnull
  private RuntimeException newUnexpectedExpressionException(@Nonnull final String expressionType) {
    return new InvalidUserInputError(
        "Unexpected expression type: " + expressionType + " in type specifier");
  }

}
