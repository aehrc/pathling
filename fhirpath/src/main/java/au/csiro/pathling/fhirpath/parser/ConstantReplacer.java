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

import static au.csiro.pathling.fhirpath.parser.Parser.buildParser;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ExternalConstantContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ExternalConstantTermContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * A special type of parser that goes through an expression and replaces any constants with the
 * corresponding values specified within a map.
 *
 * @author John Grimes
 */
public class ConstantReplacer extends FhirPathBaseVisitor<String> {

  private final Map<String, String> constants;

  public ConstantReplacer(final Map<String, String> constants) {
    this.constants = constants;
  }

  @Nonnull
  public String execute(@Nonnull final String expression) {
    return visit(buildParser(expression).expression());
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
