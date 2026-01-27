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

package au.csiro.pathling.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.rest.api.server.RequestDetails;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Felix Naumann
 */
public class MockUtil {

  public static RequestDetails mockRequest(
      String acceptHeader, String preferHeader, boolean lenient) {
    RequestDetails details = mock(RequestDetails.class);
    when(details.getHeader("Accept")).thenReturn(acceptHeader);
    List<String> accept = new ArrayList<>();
    if (acceptHeader != null) {
      accept.add(acceptHeader);
    }
    when(details.getHeaders("Accept")).thenReturn(accept);
    when(details.getHeader("Prefer")).thenReturn(preferHeader);
    List<String> prefer = new ArrayList<>();
    if (preferHeader != null) {
      prefer.add(preferHeader);
    }
    if (lenient) {
      prefer.add("handling=lenient");
    }
    when(details.getHeaders("Prefer")).thenReturn(prefer);
    when(details.getCompleteUrl()).thenReturn("test-url");
    return details;
  }
}
