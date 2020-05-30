/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import au.csiro.pathling.errors.InvalidUserInputError;
import javax.annotation.Nullable;
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
    final String errorMessage = msg == null
                                ? "Error parsing FHIRPath expression"
                                : "Error parsing FHIRPath expression: " + msg;
    throw new InvalidUserInputError(errorMessage);
  }

}
