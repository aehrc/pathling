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
    return acceptedHeaderValues().getFirst();
  }

  /**
   * Tests if the provided header value is valid.
   *
   * @param headerValue The header value to test.
   * @return True if valid, false otherwise.
   */
  public boolean validValue(final String headerValue) {
    if (headerValue == null) {
      return false;
    }
    final List<String> values = Arrays.stream(headerValue.split(",")).map(String::trim).toList();
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
  public boolean validValue(final Collection<String> headerValues) {
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
  public boolean validValue(final ServletRequestDetails request) {
    Objects.requireNonNull(request);
    return validValue(request.getHeaders(headerName()));
  }
}
