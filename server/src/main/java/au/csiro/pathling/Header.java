package au.csiro.pathling;

import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Utility class to interact with HTTP headers.
 *
 * @param headerName The header name this instance belongs to.
 * @param acceptedHeaderValues A collection of header values that are deemed valid here (usually
 *     synonyms).
 * @author Felix Naumann
 */
public record Header(String headerName, List<String> acceptedHeaderValues) {

  /**
   * With synonyms there may be one preferred value.
   *
   * @return The preferred header value.
   */
  public String preferred() {
    return acceptedHeaderValues().get(0);
  }

  /**
   * Tests if the provided header value is valid.
   *
   * @param headerValue The header value to test.
   * @return True if valid, false otherwise.
   */
  public boolean validValue(String headerValue) {
    if (headerValue == null) {
      return false;
    }
    List<String> values = Arrays.stream(headerValue.split(",")).map(String::trim).toList();
    // Accept wildcard */* which means the client accepts any content type.
    if (values.contains("*/*")) {
      return true;
    }
    return acceptedHeaderValues.stream().anyMatch(values::contains);
  }

  /**
   * Tests if the provided header values are valid. The method returns true if at least one header
   * value is valid.
   *
   * @param headerValues The header values to test.
   * @return True if at least one header value valid, false otherwise.
   */
  public boolean validValue(Collection<String> headerValues) {
    return headerValues != null && headerValues.stream().anyMatch(this::validValue);
  }

  /**
   * Tests if the provided header values are valid. The method returns true if at least one header
   * value is valid.
   *
   * @param request An entire request object. The associated header key will be used to retrieve the
   *     header values.
   * @return True if at least one header value valid, false otherwise.
   */
  public boolean validValue(ServletRequestDetails request) {
    Objects.requireNonNull(request);
    return validValue(request.getHeaders(headerName()));
  }
}
