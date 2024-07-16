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
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
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
import java.util.Optional;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * This class deals with terms that are literal expressions.
 *
 * @author John Grimes
 */
class LiteralTermVisitor extends FhirPathBaseVisitor<FhirPath> {

  @Override
  public FhirPath visitStringLiteral(final StringLiteralContext ctx) {
    final String fhirPath = ctx.getText();
    return (input, context) -> StringCollection.fromLiteral(fhirPath);
  }

  @Override
  public FhirPath visitDateLiteral(final DateLiteralContext ctx) {
    final String fhirPath = ctx.getText();
    return (input, context) -> {
      try {
        return DateCollection.fromLiteral(fhirPath);
      } catch (final ParseException e) {
        throw new FhirPathParsingError("Unable to parse date format: " + fhirPath);
      }
    };
  }

  @Override
  public FhirPath visitDateTimeLiteral(final DateTimeLiteralContext ctx) {
    final String fhirPath = ctx.getText();
    return (input, context) -> {
      try {
        return DateTimeCollection.fromLiteral(fhirPath);
      } catch (final ParseException e) {
        throw new FhirPathParsingError("Unable to parse date/time format: " + fhirPath);
      }
    };
  }

  @Override
  public FhirPath visitTimeLiteral(final TimeLiteralContext ctx) {
    final String fhirPath = ctx.getText();
    return (input, context) -> TimeCollection.fromLiteral(fhirPath);
  }

  @Override
  public FhirPath visitNumberLiteral(final NumberLiteralContext ctx) {
    final String fhirPath = ctx.getText();
    return (input, context) -> {
      // The FHIRPath grammar lumps these two types together, so we tease them apart by trying to 
      // parse them.
      try {
        return IntegerCollection.fromLiteral(fhirPath);
      } catch (final NumberFormatException e) {
        try {
          return DecimalCollection.fromLiteral(fhirPath);
        } catch (final NumberFormatException ex) {
          throw new FhirPathParsingError("Invalid date format: " + fhirPath);
        }
      }
    };
  }

  @Override
  public FhirPath visitBooleanLiteral(final BooleanLiteralContext ctx) {
    return (input, context) -> BooleanCollection.fromLiteral(ctx.getText());
  }

  @Override
  public FhirPath visitNullLiteral(final NullLiteralContext ctx) {
    return (input, context) -> new EmptyCollection();
  }

  @Override
  public FhirPath visitQuantityLiteral(final QuantityLiteralContext ctx) {
    final String number = ctx.quantity().NUMBER().getText();
    final ParserRuleContext dateTimePrecision = ctx.quantity().unit()
        .dateTimePrecision();
    final ParserRuleContext pluralDateTimePrecisionContext = ctx.quantity().unit()
        .pluralDateTimePrecision();
    final TerminalNode ucumUnit = ctx.quantity().unit().STRING();
    return (input, context) -> Optional.ofNullable(dateTimePrecision)
        .or(() -> Optional.ofNullable(pluralDateTimePrecisionContext))
        .map(p -> QuantityCollection.fromCalendarDurationLiteral(number, p.getText()))
        .orElseGet(() -> QuantityCollection.fromUcumLiteral(number, ucumUnit.getText()));
  }

  @Override
  public FhirPath visitCodingLiteral(final CodingLiteralContext ctx) {
    final TerminalNode coding = ctx.CODING();
    return (input, context) -> CodingCollection.fromLiteral(coding.getText());
  }

}
