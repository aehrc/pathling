/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.parsing.parser;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * This is a custom error listener for capturing parse errors and wrapping them in an invalid
 * request exception.
 *
 * @author John Grimes
 */
public class ExpressionParserErrorListener extends BaseErrorListener {

  @Override
  public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
      int charPositionInLine, String msg, RecognitionException e) {
    throw new InvalidRequestException("Error parsing FHIRPath expression: " + msg);
  }

}
