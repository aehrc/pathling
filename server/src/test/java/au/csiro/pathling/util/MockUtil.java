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
