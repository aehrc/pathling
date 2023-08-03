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

import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathLexer;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ExternalConstantContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ExternalConstantTermContext;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class ConstantReplacer extends FhirPathBaseVisitor<String> {

  private final Map<String, String> constants;

  public ConstantReplacer(final Map<String, String> constants) {
    this.constants = constants;
  }

  @Nonnull
  public String execute(@Nonnull final String expression) {
    final FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final FhirPathParser parser = new FhirPathParser(tokens);

    lexer.removeErrorListeners();
    lexer.addErrorListener(new ParserErrorListener());

    // Remove the default console error reporter, and add a listener that wraps each parse error in
    // an invalid request exception.
    parser.removeErrorListeners();
    parser.addErrorListener(new ParserErrorListener());

    return visit(parser.expression());
  }

  @Override
  public String visitExternalConstantTerm(@Nullable final ExternalConstantTermContext ctx) {
    final ExternalConstantContext constantContext = requireNonNull(
        requireNonNull(ctx).externalConstant());
    final String term = Optional.ofNullable((ParseTree) constantContext.identifier())
        .orElse(constantContext.STRING()).getText();

    final Optional<String> constant = Optional.ofNullable(constants.get(term));
    return constant.orElse(term);
  }

}
