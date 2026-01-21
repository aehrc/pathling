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

package au.csiro.pathling.async;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AsyncConfiguration;
import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.ResourceNotFoundError;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/**
 * Tests for {@link JobResultProvider}.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class JobResultProviderTest {

  private static final String JOB_ID = "abc12345-1234-1234-8234-123456789012";

  private JobRegistry jobRegistry;
  private JobResultProvider jobResultProvider;
  private MockHttpServletRequest request;
  private MockHttpServletResponse response;
  private ServerConfiguration config;

  @BeforeEach
  void setUp() {
    jobRegistry = new JobRegistry();
    config = mock(ServerConfiguration.class);
    final AuthorizationConfiguration authConfig = mock(AuthorizationConfiguration.class);
    when(config.getAuth()).thenReturn(authConfig);
    when(authConfig.isEnabled()).thenReturn(false);

    final AsyncConfiguration asyncConfig = mock(AsyncConfiguration.class);
    when(asyncConfig.getCacheMaxAge()).thenReturn(60);
    when(config.getAsync()).thenReturn(asyncConfig);

    jobResultProvider = new JobResultProvider(config, jobRegistry);
    request = new MockHttpServletRequest();
    request.setMethod("GET");
    response = new MockHttpServletResponse();

    // Clear security context between tests.
    SecurityContextHolder.clearContext();
  }

  @Test
  void successfulJobResultReturns200WithParameters() {
    // A completed job should return 200 OK with the Parameters result.
    final Parameters expectedResult = new Parameters();
    expectedResult.addParameter().setName("output").setValue(new StringType("test-value"));

    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(expectedResult);
    final Job<IBaseResource> job = new Job<>(JOB_ID, "view-export", future, Optional.empty());
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    final IBaseResource result = jobResultProvider.jobResult(JOB_ID, request, response);

    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_OK);
    assertThat(result).isInstanceOf(Parameters.class);
    final Parameters parameters = (Parameters) result;
    assertThat(parameters.getParameter()).hasSize(1);
    assertThat(parameters.getParameter().get(0).getName()).isEqualTo("output");
  }

  @Test
  void failedJobResultReturnsOperationOutcome() {
    // A failed job should throw an exception that gets converted to an appropriate response.
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    future.completeExceptionally(
        new IllegalStateException(
            "Problem processing request asynchronously",
            new InvalidRequestException("Test validation error")));

    final Job<IBaseResource> job = new Job<>(JOB_ID, "view-export", future, Optional.empty());
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    // The error should be converted and thrown.
    assertThatThrownBy(() -> jobResultProvider.jobResult(JOB_ID, request, response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Test validation error");
  }

  @Test
  void unknownJobIdReturns404() {
    // An unknown job ID should return 404 Not Found.
    assertThatThrownBy(
            () ->
                jobResultProvider.jobResult(
                    "unknown-id-1234-1234-8234-123456789012", request, response))
        .isInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("Job ID not found");
  }

  @Test
  void inProgressJobResultReturns400BadRequest() {
    // Attempting to get the result of an in-progress job should return 400 Bad Request.
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    final Job<IBaseResource> job = new Job<>(JOB_ID, "view-export", future, Optional.empty());
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobResultProvider.jobResult(JOB_ID, request, response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Job is not yet complete");
  }

  @Test
  void resultSetsAppropriateHeaders() {
    // Verify that the result endpoint sets appropriate cache headers.
    final Parameters expectedResult = new Parameters();
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(expectedResult);
    final Job<IBaseResource> job = new Job<>(JOB_ID, "view-export", future, Optional.empty());
    job.setRedirectOnComplete(true);
    // Set a response modification (like the Expires header from ViewDefinitionExportProvider).
    job.setResponseModification(
        httpServletResponse -> httpServletResponse.addHeader("Expires", "some-date"));
    jobRegistry.register(job);

    jobResultProvider.jobResult(JOB_ID, request, response);

    assertThat(response.getHeader("Cache-Control")).isEqualTo("max-age=60");
    assertThat(response.getHeader("Expires")).isEqualTo("some-date");
  }

  @Test
  void ownershipCheckEnforcedForResult() {
    // When auth is enabled, results should only be returned to the job owner.
    final AuthorizationConfiguration authConfig = mock(AuthorizationConfiguration.class);
    when(config.getAuth()).thenReturn(authConfig);
    when(authConfig.isEnabled()).thenReturn(true);

    // Set up authentication with a different user ID and the required authority.
    final Jwt jwt =
        Jwt.withTokenValue("token").header("alg", "none").claim("sub", "different-user").build();
    SecurityContextHolder.getContext()
        .setAuthentication(
            new JwtAuthenticationToken(
                jwt, AuthorityUtils.createAuthorityList("pathling:view-export")));

    final Parameters expectedResult = new Parameters();
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(expectedResult);
    // Job owned by "original-owner".
    final Job<IBaseResource> job =
        new Job<>(JOB_ID, "view-export", future, Optional.of("original-owner"));
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobResultProvider.jobResult(JOB_ID, request, response))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("not owned by the current user");
  }

  @Test
  void ownerMatchAllowsAccess() {
    // When auth is enabled and the user is the job owner, access should be allowed.
    final AuthorizationConfiguration authConfig = mock(AuthorizationConfiguration.class);
    when(config.getAuth()).thenReturn(authConfig);
    when(authConfig.isEnabled()).thenReturn(true);

    // Set up authentication with the same user ID as the job owner and the required authority.
    final Jwt jwt =
        Jwt.withTokenValue("token").header("alg", "none").claim("sub", "job-owner-123").build();
    SecurityContextHolder.getContext()
        .setAuthentication(
            new JwtAuthenticationToken(
                jwt, AuthorityUtils.createAuthorityList("pathling:view-export")));

    final Parameters expectedResult = new Parameters();
    expectedResult.addParameter().setName("output").setValue(new StringType("data"));
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(expectedResult);
    final Job<IBaseResource> job =
        new Job<>(JOB_ID, "view-export", future, Optional.of("job-owner-123"));
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    final IBaseResource result = jobResultProvider.jobResult(JOB_ID, request, response);

    assertThat(result).isInstanceOf(Parameters.class);
  }

  @Test
  void cancelledJobReturns404() {
    // A cancelled job should return 404 Not Found.
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    future.cancel(false);
    final Job<IBaseResource> job = new Job<>(JOB_ID, "view-export", future, Optional.empty());
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobResultProvider.jobResult(JOB_ID, request, response))
        .isInstanceOf(ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException.class)
        .hasMessageContaining("DELETE request cancelled this job");
  }

  @Test
  void interruptedJobThrowsInternalError() {
    // A job that was interrupted should throw an InternalErrorException.
    // Create a future that will throw InterruptedException when get() is called.
    @SuppressWarnings("unchecked")
    final Future<IBaseResource> mockFuture = mock(Future.class);
    try {
      when(mockFuture.isDone()).thenReturn(true);
      when(mockFuture.isCancelled()).thenReturn(false);
      when(mockFuture.get()).thenThrow(new InterruptedException("Thread was interrupted"));
    } catch (final InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    final Job<IBaseResource> job = new Job<>(JOB_ID, "view-export", mockFuture, Optional.empty());
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobResultProvider.jobResult(JOB_ID, request, response))
        .isInstanceOf(InternalErrorException.class)
        .hasMessageContaining("Job was interrupted");
  }

  @Test
  void errorUnwrappingHandlesDirectCause() {
    // Test that errors with a direct cause are properly unwrapped.
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    future.completeExceptionally(new InvalidRequestException("Direct error"));

    final Job<IBaseResource> job = new Job<>(JOB_ID, "view-export", future, Optional.empty());
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobResultProvider.jobResult(JOB_ID, request, response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Direct error");
  }

  @Test
  void errorUnwrappingHandlesNestedCause() {
    // Test that errors with a nested cause (wrapped in IllegalStateException) are properly
    // unwrapped.
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    future.completeExceptionally(
        new IllegalStateException(
            "Outer wrapper", new InvalidRequestException("Nested error message")));

    final Job<IBaseResource> job = new Job<>(JOB_ID, "view-export", future, Optional.empty());
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobResultProvider.jobResult(JOB_ID, request, response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Nested error message");
  }

  @Test
  void nullJobIdReturns404() {
    // A null job ID should return 404 Not Found.
    assertThatThrownBy(() -> jobResultProvider.jobResult(null, request, response))
        .isInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("Job ID not found");
  }

  @Test
  void invalidJobIdFormatReturns404() {
    // An invalid job ID format should return 404 Not Found.
    assertThatThrownBy(() -> jobResultProvider.jobResult("not-a-valid-uuid", request, response))
        .isInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("Job ID not found");
  }
}
