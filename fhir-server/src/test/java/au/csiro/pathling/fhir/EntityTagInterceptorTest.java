/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.Storage;
import au.csiro.pathling.Configuration.Storage.Aws;
import au.csiro.pathling.caching.EntityTagInterceptor;
import au.csiro.pathling.caching.EntityTagValidator;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.NotModifiedException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
class EntityTagInterceptorTest {

  private static final String TAG = "W/\"abc123\"";
  private static final String NON_S3_RESULT_URL = "file:///tmp/results";
  private static final String S3_RESULT_URL = "s3://somebucket/results";
  private static final long SIGNED_URL_EXPIRY = 3600L;

  private EntityTagValidator validator;
  private Storage storage;
  private HttpServletRequest request;
  private RequestDetails requestDetails;
  private HttpServletResponse response;
  private EntityTagInterceptor interceptor;

  @BeforeEach
  void setUp() {
    validator = mock(EntityTagValidator.class);
    final Configuration configuration = mock(Configuration.class);
    storage = mock(Storage.class);
    final Aws aws = mock(Aws.class);
    when(configuration.getStorage()).thenReturn(storage);
    when(storage.getAws()).thenReturn(aws);
    when(aws.getSignedUrlExpiry()).thenReturn(SIGNED_URL_EXPIRY);
    request = mock(HttpServletRequest.class);
    requestDetails = mock(RequestDetails.class);
    response = mock(HttpServletResponse.class);
    interceptor = new EntityTagInterceptor(validator, configuration);
  }

  @Test
  void setsETagAndCacheControl() {
    setupCacheableRequest("GET", null, S3_RESULT_URL, "$aggregate");
    when(validator.matches(isNull())).thenReturn(false);
    when(validator.tag()).thenReturn(TAG);

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyResponseHeaders();
  }

  @Test
  void returnsNotModified() {
    setupCacheableRequest("GET", TAG, NON_S3_RESULT_URL, "$aggregate");
    when(validator.matches(eq(TAG))).thenReturn(true);

    assertThrows(NotModifiedException.class,
        () -> interceptor.checkIncomingTag(request, requestDetails, response));

    verifyNoInteractions(response);
  }

  @Test
  void setsETagForExtractRequest() {
    setupCacheableRequest("GET", null, NON_S3_RESULT_URL, "$extract");
    when(validator.matches(isNull())).thenReturn(false);
    when(validator.tag()).thenReturn(TAG);

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyResponseHeaders();
  }

  @Test
  void setsETagForS3ExtractRequest() {
    setupCacheableRequest("GET", null, S3_RESULT_URL, "$extract");
    when(validator.matches(isNull())).thenReturn(false);
    when(validator.tagForTime(anyLong())).thenReturn(TAG);

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyResponseHeaders();
  }

  @Test
  void setsETagForSearchRequest() {
    setupCacheableRequest("GET", null, NON_S3_RESULT_URL, null);
    when(validator.matches(isNull())).thenReturn(false);
    when(validator.tag()).thenReturn(TAG);

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyResponseHeaders();
  }

  @Test
  void returnsNotModifiedForS3ExtractRequest() {
    setupCacheableRequest("GET", TAG, S3_RESULT_URL, "$extract");
    when(validator.validWithExpiry(eq(TAG), eq(SIGNED_URL_EXPIRY), anyLong())).thenReturn(true);

    assertThrows(NotModifiedException.class,
        () -> interceptor.checkIncomingTag(request, requestDetails, response));

    verifyNoInteractions(response);
  }

  @Test
  void setsETagForHead() {
    setupCacheableRequest("HEAD", null, S3_RESULT_URL, "$aggregate");
    when(validator.matches(isNull())).thenReturn(false);
    when(validator.tag()).thenReturn(TAG);

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyResponseHeaders();
  }

  @Test
  void doesNothingWhenNotCacheable() {
    setupCacheableRequest("POST", null, S3_RESULT_URL, "$aggregate");

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyNoInteractions(validator);
    verifyNoInteractions(response);
  }

  private void setupCacheableRequest(@Nonnull final String method, @Nullable final String tag,
      @Nonnull final String resultUrl, @Nullable final String operation) {
    when(request.getMethod()).thenReturn(method);
    when(request.getHeader(eq("If-None-Match"))).thenReturn(tag);
    when(storage.getResultUrl()).thenReturn(resultUrl);
    when(requestDetails.getOperation()).thenReturn(operation);
  }

  private void verifyResponseHeaders() {
    verify(response).setHeader(eq("ETag"), eq(TAG));
    verify(response).setHeader(eq("Cache-Control"), eq("must-revalidate"));
    verify(response).addHeader(eq("Cache-Control"), eq("max-age=1"));
  }

}
