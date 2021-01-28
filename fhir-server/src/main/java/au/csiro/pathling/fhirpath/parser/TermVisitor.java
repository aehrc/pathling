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
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A term is typically a standalone literal or function invocation.
 *
 * @author John Grimes
 */
class TermVisitor extends FhirPathBaseVisitor<ParserResult> {

  @Nonnull
  private final ParserContext context;

  TermVisitor(@Nonnull final ParserContext context) {
    this.context = context;
  }

  @Override
  @Nonnull
  public ParserResult visitInvocationTerm(@Nonnull final InvocationTermContext ctx) {
    return new InvocationVisitor(context).visit(ctx.invocation());
  }

  @Override
  @Nonnull
  public ParserResult visitLiteralTerm(@Nonnull final LiteralTermContext ctx) {
    return new LiteralTermVisitor(context).visit(ctx.literal());
  }

  @Override
  @Nonnull
  public ParserResult visitExternalConstantTerm(@Nonnull final ExternalConstantTermContext ctx) {
    @Nullable final String term = ctx.getText();
    checkNotNull(term);
    checkUserInput(term.equals("%resource") || term.equals("%context"),
        "Unsupported environment variable: " + term);

    // The %resource and %context elements both return the input context.
    // TODO: Find the actual root parser context


    final ParserContext evaluationContext;
    if (context.getThisContext().isPresent()) {
      System.out.println("Switchig evaluation context to root: %resource");
      evaluationContext = new ParserContext(context.getInputContext(),
          context.getFhirContext(), context.getSparkSession(),
          context.getResourceReader(), context.getTerminologyClient(),
          context.getTerminologyClientFactory(),
          // TODO: This possibly should not be empty but rather the original grupping
          // columns
          Optional.empty());
    } else {
     evaluationContext = context;
    }
    return evaluationContext.resultFor(context.getInputContext());
  }

  @Override
  @Nonnull
  public ParserResult visitParenthesizedTerm(@Nonnull final ParenthesizedTermContext ctx) {
    // Parentheses are ignored in the standalone term case.
    return new Visitor(context).visit(ctx.expression());
  }

}
