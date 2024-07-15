package au.csiro.pathling.fhirpath.parser;

import java.io.Serial;

public class UnsupportedExpressionException extends RuntimeException {

  @Serial
  private static final long serialVersionUID = 1L;

  public UnsupportedExpressionException(final String message) {
    super(message);
  }
 
}
