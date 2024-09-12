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
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.expression.CalendarDurationLiteral;
import au.csiro.pathling.fhirpath.expression.CodingLiteral;
import au.csiro.pathling.fhirpath.expression.DateLiteral;
import au.csiro.pathling.fhirpath.expression.DateTimeLiteral;
import au.csiro.pathling.fhirpath.expression.NullLiteral;
import au.csiro.pathling.fhirpath.expression.NumberLiteral;
import au.csiro.pathling.fhirpath.expression.QuantityLiteral;
import au.csiro.pathling.fhirpath.expression.StringLiteral;
import au.csiro.pathling.fhirpath.expression.TimeLiteral;
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
    return new StringLiteral(FhirPathType.STRING, ctx.getText());
  }

  @Override
  public FhirPath visitDateLiteral(final DateLiteralContext ctx) {
    return new DateLiteral(FhirPathType.DATE, ctx.getText());
  }

  @Override
  public FhirPath visitDateTimeLiteral(final DateTimeLiteralContext ctx) {
    return new DateTimeLiteral(FhirPathType.DATE_TIME, ctx.getText());
  }

  @Override
  public FhirPath visitTimeLiteral(final TimeLiteralContext ctx) {
    return new TimeLiteral(FhirPathType.TIME, ctx.getText());
  }

  @Override
  public FhirPath visitNumberLiteral(final NumberLiteralContext ctx) {
    return new NumberLiteral(ctx.getText());
  }

  @Override
  public FhirPath visitBooleanLiteral(final BooleanLiteralContext ctx) {
    return new StringLiteral(FhirPathType.BOOLEAN, ctx.getText());
  }

  @Override
  public FhirPath visitNullLiteral(final NullLiteralContext ctx) {
    return new NullLiteral();
  }

  @Override
  public FhirPath visitQuantityLiteral(final QuantityLiteralContext ctx) {
    final String number = ctx.quantity().NUMBER().getText();
    final ParserRuleContext dateTimePrecision = ctx.quantity().unit()
        .dateTimePrecision();
    final ParserRuleContext pluralDateTimePrecisionContext = ctx.quantity().unit()
        .pluralDateTimePrecision();
    final TerminalNode ucumUnit = ctx.quantity().unit().STRING();
    return Optional.ofNullable(dateTimePrecision)
        .or(() -> Optional.ofNullable(pluralDateTimePrecisionContext))
        .map(context -> (FhirPath) new CalendarDurationLiteral(number, context.getText()))
        .orElseGet(() -> new QuantityLiteral(number, ucumUnit.getText()));
  }

  @Override
  public FhirPath visitCodingLiteral(final CodingLiteralContext ctx) {
    return new CodingLiteral(FhirPathType.CODING, ctx.getText());
  }

}
