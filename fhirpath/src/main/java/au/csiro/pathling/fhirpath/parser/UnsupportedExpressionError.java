package au.csiro.pathling.fhirpath.parser;

import java.io.Serial;

public class UnsupportedExpressionError extends FhirPathParsingError {

  @Serial
  private static final long serialVersionUID = 1L;

  public UnsupportedExpressionError(final String message) {
    super(message);
  }

}
