/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.literal.*;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.*;
import java.text.ParseException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class deals with terms that are literal expressions.
 *
 * @author John Grimes
 */
class LiteralTermVisitor extends FhirPathBaseVisitor<FhirPath> {

  @Nonnull
  private final ParserContext context;

  LiteralTermVisitor(@Nonnull final ParserContext context) {
    this.context = context;
  }

  @Override
  @Nonnull
  public FhirPath visitCodingLiteral(@Nullable final CodingLiteralContext ctx) {
    @Nullable final String fhirPath = checkNotNull(ctx).getText();
    checkNotNull(fhirPath);
    try {
      return CodingLiteralPath.fromString(fhirPath,
          context.getThisContext().orElse(context.getInputContext()));
    } catch (final IllegalArgumentException e) {
      throw new InvalidUserInputError(e.getMessage(), e);
    }
  }

  @Override
  @Nonnull
  public FhirPath visitStringLiteral(@Nullable final StringLiteralContext ctx) {
    @Nullable final String fhirPath = checkNotNull(ctx).getText();
    checkNotNull(fhirPath);
    return StringLiteralPath.fromString(fhirPath,
        context.getThisContext().orElse(context.getInputContext()));
  }

  @Override
  public FhirPath visitDateLiteral(@Nullable final DateLiteralContext ctx) {
    @Nullable final String fhirPath = checkNotNull(ctx).getText();
    checkNotNull(fhirPath);
    try {
      return DateLiteralPath.fromString(fhirPath,
          context.getThisContext().orElse(context.getInputContext()));
    } catch (final ParseException ex) {
      throw new InvalidUserInputError("Unable to parse date format: " + fhirPath);
    }
  }

  @Override
  @Nonnull
  public FhirPath visitDateTimeLiteral(@Nullable final DateTimeLiteralContext ctx) {
    @Nullable final String fhirPath = checkNotNull(ctx).getText();
    checkNotNull(fhirPath);
    try {
      return DateTimeLiteralPath.fromString(fhirPath,
          context.getThisContext().orElse(context.getInputContext()));
    } catch (final ParseException ex) {
      throw new InvalidUserInputError("Unable to parse date/time format: " + fhirPath);
    }
  }

  @Override
  @Nonnull
  public FhirPath visitTimeLiteral(@Nullable final TimeLiteralContext ctx) {
    @Nullable final String fhirPath = checkNotNull(ctx).getText();
    checkNotNull(fhirPath);
    return TimeLiteralPath.fromString(fhirPath,
        context.getThisContext().orElse(context.getInputContext()));
  }

  @Override
  @Nonnull
  public FhirPath visitNumberLiteral(@Nullable final NumberLiteralContext ctx) {
    @Nullable final String fhirPath = checkNotNull(ctx).getText();
    checkNotNull(fhirPath);
    // The FHIRPath grammar lumps these two types together, so we tease them apart by trying to 
    // parse them. A better way of doing this would be to modify the grammar.
    try {
      return IntegerLiteralPath.fromString(fhirPath,
          context.getThisContext().orElse(context.getInputContext()));
    } catch (final NumberFormatException e) {
      try {
        return DecimalLiteralPath.fromString(fhirPath,
            context.getThisContext().orElse(context.getInputContext()));
      } catch (final NumberFormatException ex) {
        throw new InvalidUserInputError("Invalid date format: " + fhirPath);
      }
    }
  }

  @Override
  @Nonnull
  public FhirPath visitBooleanLiteral(@Nullable final BooleanLiteralContext ctx) {
    checkNotNull(ctx);
    @Nullable final String fhirPath = ctx.getText();
    checkNotNull(fhirPath);
    return BooleanLiteralPath.fromString(fhirPath,
        context.getThisContext().orElse(context.getInputContext()));
  }

  @Override
  @Nonnull
  public FhirPath visitNullLiteral(@Nullable final NullLiteralContext ctx) {
    return NullLiteralPath.build(context.getThisContext().orElse(context.getInputContext()));
  }

  @Override
  @Nonnull
  public FhirPath visitQuantityLiteral(@Nullable final QuantityLiteralContext ctx) {
    throw new InvalidUserInputError("Quantity literals are not supported");
  }

}
