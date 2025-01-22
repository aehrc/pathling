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
import au.csiro.pathling.fhirpath.path.Literals.BooleanLiteral;
import au.csiro.pathling.fhirpath.path.Literals.CalendarDurationLiteral;
import au.csiro.pathling.fhirpath.path.Literals.CodingLiteral;
import au.csiro.pathling.fhirpath.path.Literals.DateLiteral;
import au.csiro.pathling.fhirpath.path.Literals.DateTimeLiteral;
import au.csiro.pathling.fhirpath.path.Literals.DecimalLiteral;
import au.csiro.pathling.fhirpath.path.Literals.IntegerLiteral;
import au.csiro.pathling.fhirpath.path.Literals.NullLiteral;
import au.csiro.pathling.fhirpath.path.Literals.StringLiteral;
import au.csiro.pathling.fhirpath.path.Literals.TimeLiteral;
import au.csiro.pathling.fhirpath.path.Literals.UcumQuantityLiteral;
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
  public FhirPath visitStringLiteral(
      @Nullable final StringLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    return new StringLiteral(requireNonNull(fhirPath));
  }

  @Override
  public FhirPath visitDateLiteral(@Nullable final DateLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    return new DateLiteral(requireNonNull(fhirPath));
  }

  @Override
  @Nonnull
  public FhirPath visitDateTimeLiteral(
      @Nullable final DateTimeLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    return new DateTimeLiteral(requireNonNull(fhirPath));

  }

  @Override
  @Nonnull
  public FhirPath visitTimeLiteral(@Nullable final TimeLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    return new TimeLiteral(requireNonNull(fhirPath));
  }

  @Override
  @Nonnull
  public FhirPath visitNumberLiteral(
      @Nullable final NumberLiteralContext ctx) {
    final String fhirPath = requireNonNull(requireNonNull(ctx).getText());
    try {
      Integer.parseInt(fhirPath);
      return new IntegerLiteral(fhirPath);
    } catch (final NumberFormatException e) {
      return new DecimalLiteral(fhirPath);
    }
  }

  @Override
  @Nonnull
  public FhirPath visitBooleanLiteral(
      @Nullable final BooleanLiteralContext ctx) {
    return new BooleanLiteral(requireNonNull(requireNonNull(ctx).getText()));
  }

  @Override
  @Nonnull
  public FhirPath visitNullLiteral(@Nullable final NullLiteralContext ctx) {
    return new NullLiteral();
  }

  @Override
  @Nonnull
  public FhirPath visitQuantityLiteral(
      @Nullable final QuantityLiteralContext ctx) {
    requireNonNull(ctx);
    @Nullable final String number = ctx.quantity().NUMBER().getText();
    requireNonNull(number);
    @Nullable final TerminalNode ucumUnit = ctx.quantity().unit().STRING();
    return (ucumUnit == null)
           ? new CalendarDurationLiteral(
        String.format("%s %s", number, ctx.quantity().unit().getText()))
           : new UcumQuantityLiteral(String.format("%s %s", number, ucumUnit.getText()));
  }

  @Override
  @Nonnull
  public FhirPath visitCodingLiteral(
      @Nullable final CodingLiteralContext ctx) {
    return new CodingLiteral(requireNonNull(requireNonNull(ctx).getText()));
  }

}
