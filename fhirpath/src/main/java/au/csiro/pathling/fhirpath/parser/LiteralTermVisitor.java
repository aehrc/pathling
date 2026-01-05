/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.BooleanLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.CodingLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.DateLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.DateTimeLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.NullLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.NumberLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.QuantityLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.StringLiteralContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TimeLiteralContext;
import au.csiro.pathling.fhirpath.path.Literals;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * This class deals with terms that are literal expressions.
 *
 * @author John Grimes
 */
class LiteralTermVisitor extends FhirPathBaseVisitor<FhirPath> {

  @Override
  @Nonnull
  public FhirPath visitStringLiteral(@Nullable final StringLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    return Literals.stringLiteral(requireNonNull(fhirPath));
  }

  @Override
  public FhirPath visitDateLiteral(@Nullable final DateLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    return Literals.dateLiteral(requireNonNull(fhirPath));
  }

  @Override
  @Nonnull
  public FhirPath visitDateTimeLiteral(@Nullable final DateTimeLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    return Literals.dateTimeLiteral(requireNonNull(fhirPath));
  }

  @Override
  @Nonnull
  public FhirPath visitTimeLiteral(@Nullable final TimeLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    return Literals.timeLiteral(requireNonNull(fhirPath));
  }

  @Override
  @Nonnull
  public FhirPath visitNumberLiteral(@Nullable final NumberLiteralContext ctx) {
    return Literals.numericLiteral(requireNonNull(requireNonNull(ctx).getText()));
  }

  @Override
  @Nonnull
  public FhirPath visitBooleanLiteral(@Nullable final BooleanLiteralContext ctx) {
    return Literals.booleanLiteral(requireNonNull(requireNonNull(ctx).getText()));
  }

  @Override
  @Nonnull
  public FhirPath visitNullLiteral(@Nullable final NullLiteralContext ctx) {
    return Literals.nullLiteral();
  }

  @Override
  @Nonnull
  public FhirPath visitQuantityLiteral(@Nullable final QuantityLiteralContext ctx) {
    requireNonNull(ctx);
    @Nullable final String number = ctx.quantity().NUMBER().getText();
    requireNonNull(number);
    @Nullable final TerminalNode ucumUnit = ctx.quantity().unit().STRING();
    return (ucumUnit == null)
        ? Literals.quantityLiteral(
            requireNonNull(number), requireNonNull(ctx.quantity().unit().getText()), false)
        : Literals.quantityLiteral(
            requireNonNull(number), requireNonNull(ucumUnit.getText()), true);
  }

  @Override
  @Nonnull
  public FhirPath visitCodingLiteral(@Nullable final CodingLiteralContext ctx) {
    return Literals.codingLiteral(requireNonNull(requireNonNull(ctx).getText()));
  }
}
