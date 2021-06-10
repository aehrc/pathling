/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ExternalConstantTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.InvocationTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.LiteralTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ParenthesizedTermContext;
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
  public FhirPath visitInvocationTerm(@Nullable final InvocationTermContext ctx) {
    return new InvocationVisitor(context).visit(checkNotNull(ctx).invocation());
  }

  @Override
  @Nonnull
  public FhirPath visitLiteralTerm(@Nullable final LiteralTermContext ctx) {
    return new LiteralTermVisitor(context).visit(checkNotNull(ctx).literal());
  }

  @Override
  @Nonnull
  public FhirPath visitExternalConstantTerm(@Nullable final ExternalConstantTermContext ctx) {
    @Nullable final String term = checkNotNull(ctx).getText();
    checkNotNull(term);
    checkUserInput(term.equals("%resource") || term.equals("%context"),
        "Unsupported environment variable: " + term);
    check(context.getInputContext() instanceof NonLiteralPath);

    // The %resource and %context elements both return the input context.
    final NonLiteralPath inputContext = (NonLiteralPath) context.getInputContext();

    // In the case of %resource and %context, the new expression will be the input context with the
    // expression updated to match the external constant term.
    return inputContext.copy(term, inputContext.getDataset(), inputContext.getIdColumn(),
        inputContext.getEidColumn(), inputContext.getValueColumn(), inputContext.isSingular(),
        inputContext.getThisColumn());
  }

  @Override
  @Nonnull
  public FhirPath visitParenthesizedTerm(@Nullable final ParenthesizedTermContext ctx) {
    // Parentheses are ignored in the standalone term case.
    return new Visitor(context).visit(checkNotNull(ctx).expression());
  }

}
