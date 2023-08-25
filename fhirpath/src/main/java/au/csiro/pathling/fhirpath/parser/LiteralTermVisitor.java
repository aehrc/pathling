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

import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
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
import java.text.ParseException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.fhir.ucum.UcumException;

/**
 * This class deals with terms that are literal expressions.
 *
 * @author John Grimes
 */
class LiteralTermVisitor extends FhirPathBaseVisitor<FhirPath<Collection, Collection>> {

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitStringLiteral(
      @Nullable final StringLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    return (input, c) -> StringCollection.fromLiteral(fhirPath);
  }

  @Override
  public FhirPath<Collection, Collection> visitDateLiteral(@Nullable final DateLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    return (input, c) -> {
      try {
        return DateCollection.fromLiteral(fhirPath);
      } catch (final ParseException e) {
        throw new InvalidUserInputError("Unable to parse date format: " + fhirPath);
      }
    };
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitDateTimeLiteral(
      @Nullable final DateTimeLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    return (input, c) -> {
      try {
        return DateTimeCollection.fromLiteral(fhirPath);
      } catch (final ParseException e) {
        throw new InvalidUserInputError("Unable to parse date/time format: " + fhirPath);
      }
    };
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitTimeLiteral(@Nullable final TimeLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    return (input, c) -> TimeCollection.fromLiteral(fhirPath);
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitNumberLiteral(
      @Nullable final NumberLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    return (input, c) -> {
      // The FHIRPath grammar lumps these two types together, so we tease them apart by trying to 
      // parse them.
      try {
        return IntegerCollection.fromLiteral(fhirPath);
      } catch (final NumberFormatException e) {
        try {
          return DecimalCollection.fromLiteral(fhirPath);
        } catch (final NumberFormatException ex) {
          throw new InvalidUserInputError("Invalid date format: " + fhirPath);
        }
      }
    };
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitBooleanLiteral(
      @Nullable final BooleanLiteralContext ctx) {
    requireNonNull(ctx);
    @Nullable final String fhirPath = ctx.getText();
    requireNonNull(fhirPath);

    return (input, c) -> BooleanCollection.fromLiteral(fhirPath);
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitNullLiteral(@Nullable final NullLiteralContext ctx) {
    return (input, c) -> Collection.nullCollection();
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitQuantityLiteral(
      @Nullable final QuantityLiteralContext ctx) {
    requireNonNull(ctx);
    @Nullable final String number = ctx.quantity().NUMBER().getText();
    requireNonNull(number);
    @Nullable final TerminalNode ucumUnit = ctx.quantity().unit().STRING();

    return (input, c) -> {
      if (ucumUnit == null) {
        // Create a calendar duration literal.
        final String fhirPath = String.format("%s %s", number, ctx.quantity().unit().getText());
        return QuantityCollection.fromCalendarDurationString(fhirPath);
      } else {
        // Create a UCUM Quantity literal.
        final String fhirPath = String.format("%s %s", number, ucumUnit.getText());
        try {
          return QuantityCollection.fromUcumString(fhirPath, Ucum.service());
        } catch (final UcumException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitCodingLiteral(
      @Nullable final CodingLiteralContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    return (input, c) -> {
      try {
        return CodingCollection.fromLiteral(fhirPath);
      } catch (final IllegalArgumentException e) {
        throw new InvalidUserInputError(e.getMessage(), e);
      }
    };
  }

}
