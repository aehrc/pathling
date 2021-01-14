/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhir.FhirPathBaseVisitor;
import au.csiro.pathling.fhir.FhirPathParser.ExternalConstantTermContext;
import au.csiro.pathling.fhir.FhirPathParser.InvocationTermContext;
import au.csiro.pathling.fhir.FhirPathParser.LiteralTermContext;
import au.csiro.pathling.fhir.FhirPathParser.ParenthesizedTermContext;
import au.csiro.pathling.fhirpath.FhirPath;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A term is typically a standalone literal or function invocation.
 *
 * @author John Grimes
 */
class TermVisitor extends FhirPathBaseVisitor<FhirPath> {

  @Nonnull
  private final ParserContext context;

  TermVisitor(@Nonnull final ParserContext context) {
    this.context = context;
  }

  @Override
  @Nonnull
  public FhirPath visitInvocationTerm(@Nonnull final InvocationTermContext ctx) {
    return new InvocationVisitor(context).visit(ctx.invocation());
  }

  @Override
  @Nonnull
  public FhirPath visitLiteralTerm(@Nonnull final LiteralTermContext ctx) {
    return new LiteralTermVisitor(context).visit(ctx.literal());
  }

  @Override
  @Nonnull
  public FhirPath visitExternalConstantTerm(@Nonnull final ExternalConstantTermContext ctx) {
    @Nullable final String term = ctx.getText();
    checkNotNull(term);
    checkUserInput(term.equals("%resource") || term.equals("%context"),
        "Unsupported environment variable: " + term);

    // The %resource and %context elements both return the input context.
    return context.getInputContext();
  }

  @Override
  @Nonnull
  public FhirPath visitParenthesizedTerm(@Nonnull final ParenthesizedTermContext ctx) {
    // Parentheses are ignored in the standalone term case.
    return new Visitor(context).visit(ctx.expression());
  }

}
