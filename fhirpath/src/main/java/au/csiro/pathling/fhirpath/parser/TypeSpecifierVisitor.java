/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.AdditiveExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.AndExpressionContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.BooleanLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.CodingLiteralContext;
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
import au.csiro.pathling.fhirpath.path.ParserPaths.TypeNamespacePath;
import au.csiro.pathling.fhirpath.path.ParserPaths.TypeSpecifierPath;
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
    return isNamespace
        ? new TypeNamespacePath(ctx.getText())
        : new TypeSpecifierPath(new TypeSpecifier(ctx.getText()));
  }

  @Override
  public FhirPath visitInvocationExpression(final InvocationExpressionContext ctx) {
    // If we are not already in a namespace and there is an invocation, we need to parse the
    // right-hand side of the invocation within the namespace.
    if (!isNamespace) {
      final TypeNamespacePath typeNamespacePath =
          (TypeNamespacePath) ctx.expression().accept(new TypeSpecifierVisitor(true));
      final String namespace = typeNamespacePath.getValue();
      final String typeName = ctx.invocation().getText();
      return new TypeSpecifierPath(new TypeSpecifier(namespace, typeName));
    } else {
      throw newUnexpectedExpressionException("InvocationExpression");
    }
  }

  @Override
  public FhirPath visitIndexerExpression(final IndexerExpressionContext ctx) {
    throw newUnexpectedExpressionException("IndexerExpression");
  }

  @Override
  public FhirPath visitPolarityExpression(final PolarityExpressionContext ctx) {
    throw newUnexpectedExpressionException("PolarityExpression");
  }

  @Override
  public FhirPath visitAdditiveExpression(final AdditiveExpressionContext ctx) {
    throw newUnexpectedExpressionException("AdditiveExpression");
  }

  @Override
  public FhirPath visitMultiplicativeExpression(final MultiplicativeExpressionContext ctx) {
    throw newUnexpectedExpressionException("MultiplicativeExpression");
  }

  @Override
  public FhirPath visitUnionExpression(final UnionExpressionContext ctx) {
    throw newUnexpectedExpressionException("UnionExpression");
  }

  @Override
  public FhirPath visitOrExpression(final OrExpressionContext ctx) {
    throw newUnexpectedExpressionException("OrExpression");
  }

  @Override
  public FhirPath visitAndExpression(final AndExpressionContext ctx) {
    throw newUnexpectedExpressionException("AndExpression");
  }

  @Override
  public FhirPath visitMembershipExpression(final MembershipExpressionContext ctx) {
    throw newUnexpectedExpressionException("MembershipExpression");
  }

  @Override
  public FhirPath visitInequalityExpression(final InequalityExpressionContext ctx) {
    throw newUnexpectedExpressionException("InequalityExpression");
  }

  @Override
  public FhirPath visitEqualityExpression(final EqualityExpressionContext ctx) {
    throw newUnexpectedExpressionException("EqualityExpression");
  }

  @Override
  public FhirPath visitImpliesExpression(final ImpliesExpressionContext ctx) {
    throw newUnexpectedExpressionException("ImpliesExpression");
  }

  @Override
  public FhirPath visitTermExpression(final TermExpressionContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public FhirPath visitTypeExpression(final TypeExpressionContext ctx) {
    throw newUnexpectedExpressionException("TypeExpression");
  }

  @Override
  public FhirPath visitInvocationTerm(final InvocationTermContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public FhirPath visitLiteralTerm(final LiteralTermContext ctx) {
    throw newUnexpectedExpressionException("LiteralTerm");
  }

  @Override
  public FhirPath visitExternalConstantTerm(final ExternalConstantTermContext ctx) {
    throw newUnexpectedExpressionException("ExternalConstantTerm");
  }

  @Override
  public FhirPath visitParenthesizedTerm(final ParenthesizedTermContext ctx) {
    throw newUnexpectedExpressionException("ParenthesizedTerm");
  }

  @Override
  public FhirPath visitNullLiteral(final NullLiteralContext ctx) {
    throw newUnexpectedExpressionException("NullLiteral");
  }

  @Override
  public FhirPath visitBooleanLiteral(final BooleanLiteralContext ctx) {
    throw newUnexpectedExpressionException("BooleanLiteral");
  }

  @Override
  public FhirPath visitStringLiteral(final StringLiteralContext ctx) {
    throw newUnexpectedExpressionException("StringLiteral");
  }

  @Override
  public FhirPath visitNumberLiteral(final NumberLiteralContext ctx) {
    throw newUnexpectedExpressionException("NumberLiteral");
  }

  @Override
  public FhirPath visitDateLiteral(final DateLiteralContext ctx) {
    throw newUnexpectedExpressionException("DateLiteral");
  }

  @Override
  public FhirPath visitDateTimeLiteral(final DateTimeLiteralContext ctx) {
    throw newUnexpectedExpressionException("DateTimeLiteral");
  }

  @Override
  public FhirPath visitTimeLiteral(final TimeLiteralContext ctx) {
    throw newUnexpectedExpressionException("TimeLiteral");
  }

  @Override
  public FhirPath visitQuantityLiteral(final QuantityLiteralContext ctx) {
    throw newUnexpectedExpressionException("QuantityLiteral");
  }

  @Override
  public FhirPath visitCodingLiteral(final CodingLiteralContext ctx) {
    throw newUnexpectedExpressionException("CodingLiteral");
  }

  @Override
  public FhirPath visitExternalConstant(final ExternalConstantContext ctx) {
    throw newUnexpectedExpressionException("ExternalConstant");
  }

  @Override
  public FhirPath visitMemberInvocation(final MemberInvocationContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public FhirPath visitFunctionInvocation(final FunctionInvocationContext ctx) {
    throw newUnexpectedExpressionException("FunctionInvocation");
  }

  @Override
  public FhirPath visitThisInvocation(final ThisInvocationContext ctx) {
    throw newUnexpectedExpressionException("ThisInvocation");
  }

  @Override
  public FhirPath visitIndexInvocation(final IndexInvocationContext ctx) {
    throw newUnexpectedExpressionException("IndexInvocation");
  }

  @Override
  public FhirPath visitTotalInvocation(final TotalInvocationContext ctx) {
    throw newUnexpectedExpressionException("TotalInvocation");
  }

  @Override
  public FhirPath visitFunction(final FunctionContext ctx) {
    throw newUnexpectedExpressionException("Function");
  }

  @Override
  public FhirPath visitParamList(final ParamListContext ctx) {
    throw newUnexpectedExpressionException("ParamList");
  }

  @Override
  public FhirPath visitQuantity(final QuantityContext ctx) {
    throw newUnexpectedExpressionException("Quantity");
  }

  @Override
  public FhirPath visitUnit(final UnitContext ctx) {
    throw newUnexpectedExpressionException("Unit");
  }

  @Override
  public FhirPath visitDateTimePrecision(final DateTimePrecisionContext ctx) {
    throw newUnexpectedExpressionException("DateTimePrecision");
  }

  @Override
  public FhirPath visitPluralDateTimePrecision(final PluralDateTimePrecisionContext ctx) {
    throw newUnexpectedExpressionException("PluralDateTimePrecision");
  }

  @Override
  public FhirPath visitTypeSpecifier(final TypeSpecifierContext ctx) {
    throw newUnexpectedExpressionException("TypeSpecifier");
  }

  @Override
  public FhirPath visitQualifiedIdentifier(final QualifiedIdentifierContext ctx) {
    throw newUnexpectedExpressionException("QualifiedIdentifier");
  }

  @Nonnull
  private RuntimeException newUnexpectedExpressionException(@Nonnull final String expressionType) {
    return new InvalidUserInputError(
        "Unexpected expression type: " + expressionType + " in type specifier");
  }
}
