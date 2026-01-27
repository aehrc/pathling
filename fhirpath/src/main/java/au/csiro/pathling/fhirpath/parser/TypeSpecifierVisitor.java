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

import static java.util.Objects.nonNull;

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
import jakarta.annotation.Nullable;
import java.util.List;

/**
 * A special visitor for the type specifiers arguments in the FHIRPath function invocations.
 *
 * @author Piotr Szul
 */
class TypeSpecifierVisitor extends FhirPathBaseVisitor<FhirPath> {

  private final boolean isNamespace;

  @Nullable final String namespace;

  private TypeSpecifierVisitor(final boolean isNamespace, @Nullable final String namespace) {
    this.isNamespace = isNamespace;
    this.namespace = namespace;
  }

  /**
   * Creates a visitor for parsing unqualified type specifiers.
   *
   * <p>This is the default visitor used for parsing type specifiers that may or may not have a
   * namespace qualifier (e.g., {@code String}, {@code FHIR.Patient}).
   *
   * @return a new visitor instance for unqualified type specifiers
   */
  @Nonnull
  static TypeSpecifierVisitor defaultVisitor() {
    return new TypeSpecifierVisitor(false, null);
  }

  /**
   * Creates a visitor for parsing namespace identifiers.
   *
   * <p>This visitor is used when parsing the left-hand side of a qualified type specifier (e.g.,
   * the {@code FHIR} part of {@code FHIR.Patient}).
   *
   * @return a new visitor instance for namespace identifiers
   */
  @Nonnull
  static TypeSpecifierVisitor namespaceVisitor() {
    return new TypeSpecifierVisitor(true, null);
  }

  /**
   * Creates a visitor for parsing type names within a specific namespace.
   *
   * <p>This visitor is used when parsing the right-hand side of a qualified type specifier (e.g.,
   * the {@code Patient} part of {@code FHIR.Patient}), where the namespace has already been
   * determined.
   *
   * @param namespace the namespace for the type specifier
   * @return a new visitor instance for qualified type names
   */
  @Nonnull
  static TypeSpecifierVisitor qualifiedVisitor(@Nonnull final String namespace) {
    return new TypeSpecifierVisitor(false, namespace);
  }

  /**
   * Visits an identifier context and returns the appropriate type specifier path.
   *
   * <p>This method handles both regular and delimited (backtick-quoted) identifiers, and produces
   * different results based on the visitor's state:
   *
   * <ul>
   *   <li>If {@code isNamespace} is true, returns a {@link TypeNamespacePath}
   *   <li>If a {@code namespace} is set, returns a qualified {@link TypeSpecifierPath}
   *   <li>Otherwise, returns an unqualified {@link TypeSpecifierPath}
   * </ul>
   *
   * @param ctx the identifier context
   * @return the appropriate type specifier or namespace path
   */
  @Override
  public FhirPath visitIdentifier(final IdentifierContext ctx) {
    final String identifier = IdentifierHelper.getIdentifierValue(ctx);

    if (isNamespace) {
      return new TypeNamespacePath(identifier);
    } else {
      return nonNull(namespace)
          ? new TypeSpecifierPath(new TypeSpecifier(namespace, identifier))
          : new TypeSpecifierPath(new TypeSpecifier(identifier));
    }
  }

  /**
   * Visits an invocation expression to parse qualified type specifiers.
   *
   * <p>This method handles expressions like {@code FHIR.Patient} or {@code System.String} by:
   *
   * <ol>
   *   <li>Parsing the left-hand side as a namespace using {@link #namespaceVisitor()}
   *   <li>Parsing the right-hand side as a type name within that namespace using {@link
   *       #qualifiedVisitor(String)}
   * </ol>
   *
   * <p>This method should only be called when {@code isNamespace} is false. If called when {@code
   * isNamespace} is true, it throws an error as nested namespace qualifications are not valid.
   *
   * @param ctx the invocation expression context
   * @return a qualified type specifier path
   * @throws InvalidUserInputError if invoked when already parsing a namespace
   */
  @Override
  public FhirPath visitInvocationExpression(final InvocationExpressionContext ctx) {
    if (!isNamespace) {
      final TypeNamespacePath typeNamespacePath =
          (TypeNamespacePath) ctx.expression().accept(TypeSpecifierVisitor.namespaceVisitor());
      final String namespace = typeNamespacePath.getValue();
      return ctx.invocation().accept(TypeSpecifierVisitor.qualifiedVisitor(namespace));
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
    // Delegate to visiting the qualified identifier within the type specifier
    return visitChildren(ctx);
  }

  @Override
  public FhirPath visitQualifiedIdentifier(final QualifiedIdentifierContext ctx) {
    // A qualified identifier is a sequence of identifiers separated by dots
    // For example: "FHIR.Patient" or "System.String" or just "String"
    // We need to parse this as either a namespace + type or just a type

    final List<IdentifierContext> identifiers = ctx.identifier();

    if (identifiers.size() == 1) {
      // Unqualified identifier: just the type name
      return visitIdentifier(identifiers.getFirst());
    } else if (identifiers.size() == 2) {
      // Qualified identifier: namespace.typename
      final String actualNamespace = IdentifierHelper.getIdentifierValue(identifiers.get(0));
      final String actualTypeName = IdentifierHelper.getIdentifierValue(identifiers.get(1));
      return new TypeSpecifierPath(new TypeSpecifier(actualNamespace, actualTypeName));
    } else {
      // More than 2 identifiers is not supported for type specifiers
      throw new InvalidUserInputError(
          "Type specifier can have at most two parts (namespace.type), got: " + ctx.getText());
    }
  }

  @Nonnull
  private RuntimeException newUnexpectedExpressionException(@Nonnull final String expressionType) {
    return new InvalidUserInputError(
        "Unexpected expression type: " + expressionType + " in type specifier");
  }
}
