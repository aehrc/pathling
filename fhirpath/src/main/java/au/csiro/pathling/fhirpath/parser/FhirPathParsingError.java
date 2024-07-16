package au.csiro.pathling.fhirpath.parser;

import java.io.Serial;

public class FhirPathParsingError extends RuntimeException {

  @Serial
  private static final long serialVersionUID = 1L;

  public FhirPathParsingError(final String message) {
    super(message);
  }

}
