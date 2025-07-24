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
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ExternalConstantContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ExternalConstantTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.InvocationTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.LiteralTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ParenthesizedTermContext;
import au.csiro.pathling.fhirpath.path.Paths.ExternalConstantPath;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * A term is typically a standalone literal or function invocation.
 *
 * @author John Grimes
 */
class TermVisitor extends FhirPathBaseVisitor<FhirPath> {

  @Override
  @Nonnull
  public FhirPath visitInvocationTerm(
      @Nullable final InvocationTermContext ctx) {
    return new InvocationVisitor(true).visit(requireNonNull(ctx).invocation());
  }

  @Override
  @Nonnull
  public FhirPath visitLiteralTerm(@Nullable final LiteralTermContext ctx) {
    return new LiteralTermVisitor().visit(requireNonNull(ctx).literal());
  }

  @Override
  @Nonnull
  public FhirPath visitExternalConstantTerm(
      @Nullable final ExternalConstantTermContext ctx) {
    final ExternalConstantContext constantContext = requireNonNull(
        requireNonNull(ctx).externalConstant());
    String term = Optional.ofNullable((ParseTree) constantContext.identifier())
        .orElse(constantContext.STRING()).getText();
    requireNonNull(term);

    // Trim any backticks or single quotes from the start and end of the term.
    term = term.replaceAll("^[`']", "");
    term = term.replaceAll("[`']$", "");

    return new ExternalConstantPath(term);
  }

  @Override
  @Nonnull
  public FhirPath visitParenthesizedTerm(
      @Nullable final ParenthesizedTermContext ctx) {
    // TODO: maybe we do not need that and just use the subExpression directly?
    // Parentheses are ignored in the standalone term case.
    final FhirPath subExpression = new Visitor().visit(
        requireNonNull(ctx).expression());
    return subExpression;
  }
}
