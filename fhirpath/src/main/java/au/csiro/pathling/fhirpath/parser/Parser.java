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
import au.csiro.pathling.fhirpath.parser.generated.FhirPathLexer;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser;
import jakarta.annotation.Nonnull;
import lombok.Getter;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * This is an ANTLR-based parser for processing a FHIRPath expression, and aggregating the results
 * into a FhirPath object. It delegates processing to a number of visitor classes, which contain the
 * logic for parsing specific parts of the grammar.
 *
 * @author John Grimes
 */
@Getter
public class Parser {

  @Nonnull
  private final ParserContext context;

  /**
   * @param context The {@link ParserContext} that this parser should use when parsing expressions
   */
  public Parser(@Nonnull final ParserContext context) {
    this.context = context;
  }

  /**
   * Parses a FHIRPath expression.
   *
   * @param expression The String representation of the FHIRPath expression
   * @return a new {@link FhirPath} object
   */
  @Nonnull
  public FhirPath parse(@Nonnull final String expression) {
    final FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final FhirPathParser parser = new FhirPathParser(tokens);

    lexer.removeErrorListeners();
    lexer.addErrorListener(new ParserErrorListener());

    // Remove the default console error reporter, and add a listener that wraps each parse error in
    // an invalid request exception.
    parser.removeErrorListeners();
    parser.addErrorListener(new ParserErrorListener());

    final Visitor visitor = new Visitor(context);
    return visitor.visit(parser.expression());
  }

}
