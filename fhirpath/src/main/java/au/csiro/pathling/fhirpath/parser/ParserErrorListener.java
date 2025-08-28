/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.errors.InvalidUserInputError;
import jakarta.annotation.Nullable;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * This is a custom error listener for capturing parse errors and wrapping them in an invalid
 * request exception.
 *
 * @author John Grimes
 */
public class ParserErrorListener extends BaseErrorListener {

  @Override
  public void syntaxError(@Nullable final Recognizer<?, ?> recognizer,
      @Nullable final Object offendingSymbol, final int line, final int charPositionInLine,
      @Nullable final String msg, @Nullable final RecognitionException e) {
    final StringBuilder sb = new StringBuilder();
    sb.append("Error parsing FHIRPath expression (line: ");
    sb.append(line);
    sb.append(", position: ");
    sb.append(charPositionInLine);
    sb.append(")");
    if (msg != null) {
      sb.append(": ");
      sb.append(msg);
    }
    throw new InvalidUserInputError(sb.toString());
  }

}
