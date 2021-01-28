/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.FhirPathBaseVisitor;
import au.csiro.pathling.fhir.FhirPathParser.*;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.literal.*;
import java.text.ParseException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class deals with terms that are literal expressions.
 *
 * @author John Grimes
 */
class LiteralTermVisitor extends FhirPathBaseVisitor<ParserResult> {

  @Nonnull
  private final ParserContext context;

  LiteralTermVisitor(@Nonnull final ParserContext context) {
    this.context = context;
  }

  @Override
  @Nonnull
  public ParserResult visitCodingLiteral(@Nonnull final CodingLiteralContext ctx) {
    @Nullable final String fhirPath = ctx.getText();
    checkNotNull(fhirPath);
    try {
      return context.resultFor(CodingLiteralPath.fromString(fhirPath, context.getInputContext()));
    } catch (final IllegalArgumentException e) {
      throw new InvalidUserInputError(e.getMessage(), e);
    }
  }

  @Override
  @Nonnull
  public ParserResult visitStringLiteral(@Nonnull final StringLiteralContext ctx) {
    @Nullable final String fhirPath = ctx.getText();
    checkNotNull(fhirPath);
    return context.resultFor(StringLiteralPath.fromString(fhirPath, context.getInputContext()));
  }

  @Override
  @Nonnull
  public ParserResult visitDateTimeLiteral(@Nonnull final DateTimeLiteralContext ctx) {
    @Nullable final String fhirPath = ctx.getText();
    checkNotNull(fhirPath);
    // The FHIRPath grammar lumps these two types together, so we tease them apart by trying to 
    // parse them. A better way of doing this would be to modify the grammar.
    try {
      return context.resultFor(DateTimeLiteralPath.fromString(fhirPath, context.getInputContext()));
    } catch (final ParseException e) {
      try {
        return context.resultFor(DateLiteralPath.fromString(fhirPath, context.getInputContext()));
      } catch (final ParseException ex) {
        throw new InvalidUserInputError("Invalid date format: " + fhirPath);
      }
    }
  }

  @Override
  @Nonnull
  public ParserResult visitTimeLiteral(@Nonnull final TimeLiteralContext ctx) {
    @Nullable final String fhirPath = ctx.getText();
    checkNotNull(fhirPath);
    return context.resultFor(TimeLiteralPath.fromString(fhirPath, context.getInputContext()));
  }

  @Override
  @Nonnull
  public ParserResult visitNumberLiteral(@Nonnull final NumberLiteralContext ctx) {
    @Nullable final String fhirPath = ctx.getText();
    checkNotNull(fhirPath);
    // The FHIRPath grammar lumps these two types together, so we tease them apart by trying to 
    // parse them. A better way of doing this would be to modify the grammar.
    try {
      return context.resultFor(IntegerLiteralPath.fromString(fhirPath, context.getInputContext()));
    } catch (final NumberFormatException e) {
      try {
        return context.resultFor(DecimalLiteralPath.fromString(fhirPath, context.getInputContext()));
      } catch (final NumberFormatException ex) {
        throw new InvalidUserInputError("Invalid date format: " + fhirPath);
      }
    }
  }

  @Override
  @Nonnull
  public ParserResult visitBooleanLiteral(@Nonnull final BooleanLiteralContext ctx) {
    @Nullable final String fhirPath = ctx.getText();
    checkNotNull(fhirPath);
    return context.resultFor(BooleanLiteralPath.fromString(fhirPath, context.getInputContext()));
  }

  @Override
  @Nonnull
  public ParserResult visitNullLiteral(@Nonnull final NullLiteralContext ctx) {
    return context.resultFor(NullLiteralPath.build(context.getInputContext()));
  }

  @Override
  @Nonnull
  public ParserResult visitQuantityLiteral(@Nonnull final QuantityLiteralContext ctx) {
    throw new InvalidUserInputError("Quantity literals are not supported");
  }

}
