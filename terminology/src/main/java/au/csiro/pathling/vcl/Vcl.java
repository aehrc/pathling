/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.vcl;

import au.csiro.pathling.vcl.generated.VclLexer;
import au.csiro.pathling.vcl.generated.VclParser;
import jakarta.annotation.Nonnull;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * Entry point for parsing VCL (ValueSet Compose Language) expressions into a {@link VclExpression}
 * abstract syntax tree.
 *
 * @author John Grimes
 */
public final class Vcl {

  private Vcl() {
    // Utility class.
  }

  /**
   * Parses a VCL expression into its abstract syntax tree.
   *
   * @param expression the VCL expression (already percent-decoded if it came from a URL)
   * @return the parsed expression tree
   * @throws VclParseException if the expression is malformed, reporting the position and reason
   */
  @Nonnull
  public static VclExpression parse(@Nonnull final String expression) {
    final BaseErrorListener errorListener = new ThrowingErrorListener();

    final VclLexer lexer = new VclLexer(CharStreams.fromString(expression));
    lexer.removeErrorListeners();
    lexer.addErrorListener(errorListener);

    final VclParser parser = new VclParser(new CommonTokenStream(lexer));
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);

    return new VclModelBuilder().build(parser.vcl());
  }

  /** An ANTLR error listener that fails fast by raising a {@link VclParseException}. */
  private static class ThrowingErrorListener extends BaseErrorListener {

    @Override
    public void syntaxError(
        final Recognizer<?, ?> recognizer,
        final Object offendingSymbol,
        final int line,
        final int charPositionInLine,
        final String msg,
        final RecognitionException e) {
      // VCL expressions are single-line, so the character offset within the line is the position.
      throw new VclParseException(charPositionInLine + 1, msg);
    }
  }
}
