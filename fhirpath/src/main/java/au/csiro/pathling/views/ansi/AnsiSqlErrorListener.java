package au.csiro.pathling.views.ansi;

import au.csiro.pathling.errors.InvalidUserInputError;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * Custom error listener for ANTLR that throws an InvalidUserInputError when a syntax error occurs.
 * This is used to handle errors in parsing ANSI SQL types.
 */
class AnsiSqlErrorListener extends BaseErrorListener {

  @Override
  public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol,
      final int line, final int charPositionInLine, final String msg,
      final RecognitionException e) {
    throw new InvalidUserInputError("Error parsing ANSI SQL type: " + msg);
  }
}
