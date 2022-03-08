/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.HttpCaching;
import au.csiro.pathling.caching.EntityTagInterceptor;
import au.csiro.pathling.caching.EntityTagValidator;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.NotModifiedException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class EntityTagInterceptorTest {

  static final String TAG = "W/\"abc123\"";

  EntityTagValidator validator;
  HttpServletRequest request;
  RequestDetails requestDetails;
  HttpServletResponse response;
  EntityTagInterceptor interceptor;

  @BeforeEach
  void setUp() {
    validator = mock(EntityTagValidator.class);
    request = mock(HttpServletRequest.class);
    requestDetails = mock(RequestDetails.class);
    response = mock(HttpServletResponse.class);
    final Configuration configuration = mock(Configuration.class);
    final HttpCaching httpCaching = mock(HttpCaching.class);
    when(httpCaching.getVary()).thenReturn(
        List.of("Accept", "Accept-Encoding", "Prefer", "Authorization"));
    when(httpCaching.getCacheableControl()).thenReturn(List.of("must-revalidate", "max-age=1"));
    when(configuration.getHttpCaching()).thenReturn(httpCaching);
    interceptor = new EntityTagInterceptor(validator, configuration);
  }

  @Test
  void setsETagAndCacheControl() {
    setupCacheableRequest("GET", null, "$aggregate");
    when(validator.matches(isNull())).thenReturn(false);
    when(validator.tag()).thenReturn(TAG);

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyMissResponseHeaders();
  }

  @Test
  void returnsNotModified() {
    setupCacheableRequest("GET", TAG, "$aggregate");
    when(validator.matches(eq(TAG))).thenReturn(true);

    assertThrows(NotModifiedException.class,
        () -> interceptor.checkIncomingTag(request, requestDetails, response));

    verifyCacheableResponseHeaders();
  }

  @Test
  void setsETagForExtractRequest() {
    setupCacheableRequest("GET", null, "$extract");
    when(validator.matches(isNull())).thenReturn(false);
    when(validator.tag()).thenReturn(TAG);

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyMissResponseHeaders();
  }

  @Test
  void setsETagForSearchRequest() {
    setupCacheableRequest("GET", null, null);
    when(validator.matches(isNull())).thenReturn(false);
    when(validator.tag()).thenReturn(TAG);

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyMissResponseHeaders();
  }

  @Test
  void setsETagForHead() {
    setupCacheableRequest("HEAD", null, "$aggregate");
    when(validator.matches(isNull())).thenReturn(false);
    when(validator.tag()).thenReturn(TAG);

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyMissResponseHeaders();
  }

  @Test
  void doesNothingWhenNotCacheable() {
    setupCacheableRequest("POST", null, "$aggregate");

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyNoInteractions(validator);
    verifyNoInteractions(response);
  }

  void setupCacheableRequest(@Nonnull final String method, @Nullable final String tag,
      @Nullable final String operation) {
    when(request.getMethod()).thenReturn(method);
    when(request.getHeader(eq("If-None-Match"))).thenReturn(tag);
    when(requestDetails.getOperation()).thenReturn(operation);
  }

  void verifyMissResponseHeaders() {
    verifyCacheableResponseHeaders();
    verify(response).setHeader(eq("ETag"), eq(TAG));
    verify(response).setHeader(eq("Cache-Control"), eq("must-revalidate,max-age=1"));
  }

  void verifyCacheableResponseHeaders() {
    verify(response).addHeader(eq("Vary"), eq("Accept,Accept-Encoding,Prefer,Authorization"));
  }

}
