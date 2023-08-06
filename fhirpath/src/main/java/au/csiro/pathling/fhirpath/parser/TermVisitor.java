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
import au.csiro.pathling.fhirpath.FhirPathTransformation;
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
class TermVisitor extends FhirPathBaseVisitor<FhirPathTransformation> {

  @Nonnull
  private final ParserContext context;

  TermVisitor(@Nonnull final ParserContext context) {
    this.context = context;
  }

  @Override
  @Nonnull
  public FhirPathTransformation visitInvocationTerm(@Nullable final InvocationTermContext ctx) {
    return new InvocationVisitor(context).visit(requireNonNull(ctx).invocation());
  }

  @Override
  @Nonnull
  public FhirPathTransformation visitLiteralTerm(@Nullable final LiteralTermContext ctx) {
    return new LiteralTermVisitor().visit(requireNonNull(ctx).literal());
  }

  @Override
  @Nonnull
  public FhirPathTransformation visitExternalConstantTerm(
      @Nullable final ExternalConstantTermContext ctx) {
    @Nullable final String term = requireNonNull(ctx).getText();
    requireNonNull(term);

    return input -> {
      if (term.equals("%context")) {
        return context.getInputContext();
      } else if (term.equals("%resource") || term.equals("%rootResource")) {
        return context.getResource();
      } else {
        throw new IllegalArgumentException("Unknown constant: " + term);
      }
    };
  }

  @Override
  @Nonnull
  public FhirPathTransformation visitParenthesizedTerm(
      @Nullable final ParenthesizedTermContext ctx) {
    return input -> {
      // Parentheses are ignored in the standalone term case.
      final FhirPath result = new Visitor(context).visit(
          requireNonNull(ctx).expression()).apply(input);
      return result.withExpression("(" + result.getExpression() + ")");
    };
  }

}
