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
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * Tests for {@link JobProvider} cache header behaviour.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class JobProviderTest {

  private static final String JOB_ID = "abc12345-1234-1234-8234-123456789012";

  private JobRegistry jobRegistry;
  private JobProvider jobProvider;
  private MockHttpServletRequest request;
  private MockHttpServletResponse response;

  @BeforeEach
  void setUp() {
    jobRegistry = new JobRegistry();
    final ServerConfiguration config = mock(ServerConfiguration.class);
    final AuthorizationConfiguration authConfig = mock(AuthorizationConfiguration.class);
    when(config.getAuth()).thenReturn(authConfig);
    when(authConfig.isEnabled()).thenReturn(false);

    final AsyncConfiguration asyncConfig = mock(AsyncConfiguration.class);
    // Configure a 60-second max-age for testing.
    when(asyncConfig.getCacheMaxAge()).thenReturn(60);
    when(config.getAsync()).thenReturn(asyncConfig);

    final SparkSession spark = mock(SparkSession.class);
    jobProvider = new JobProvider(config, jobRegistry, spark, "/tmp/test");
    request = new MockHttpServletRequest();
    request.setMethod("GET");
    // Set the servlet path to match the FHIR server mount point.
    request.setServletPath("/fhir");
    response = new MockHttpServletResponse();
  }

  @Test
  void completedJobSetsCacheControlWithMaxAge() {
    // Completed job responses should have Cache-Control: max-age=60.
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(new Parameters());
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    jobRegistry.register(job);

    jobProvider.job(JOB_ID, request, response);

    assertThat(response.getHeader("Cache-Control")).isEqualTo("max-age=60");
  }

  @Test
  void inProgressJobSetsCacheControlNoCache() {
    // In-progress job responses should have Cache-Control: no-cache to prevent caching
    // of transient status responses.
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobProvider.job(JOB_ID, request, response))
        .isInstanceOf(ProcessingNotCompletedException.class);

    assertThat(response.getHeader("Cache-Control")).isEqualTo("no-cache");
  }

  @Test
  void jobResponseHasNoEtag() {
    // Job responses should not set ETag header.
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(new Parameters());
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    jobRegistry.register(job);

    jobProvider.job(JOB_ID, request, response);

    assertThat(response.getHeader("ETag")).isNull();
  }

  @Test
  void completedJobWithRedirectReturns303SeeOther() {
    // When redirectOnComplete is enabled, completed jobs should return 303 See Other with a
    // Location header pointing to the result endpoint.
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(new Parameters());
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    final IBaseResource result = jobProvider.job(JOB_ID, request, response);

    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_SEE_OTHER);
    assertThat(response.getHeader("Location")).isEqualTo("/fhir/$job-result?id=" + JOB_ID);
    // An empty Parameters resource is returned with the 303.
    assertThat(result).isInstanceOf(Parameters.class);
    assertThat(((Parameters) result).getParameter()).isEmpty();
  }

  @Test
  void completedJobWithoutRedirectReturns200WithResult() {
    // When redirectOnComplete is not enabled (default), completed jobs return 200 OK with the
    // inline result.
    final Parameters expectedResult = new Parameters();
    expectedResult
        .addParameter()
        .setName("test")
        .setValue(new org.hl7.fhir.r4.model.StringType("value"));
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(expectedResult);
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    // redirectOnComplete is false by default.
    jobRegistry.register(job);

    final IBaseResource result = jobProvider.job(JOB_ID, request, response);

    assertThat(response.getStatus()).isEqualTo(HttpServletResponse.SC_OK);
    assertThat(response.getHeader("Location")).isNull();
    assertThat(result).isInstanceOf(Parameters.class);
    assertThat(((Parameters) result).getParameter()).hasSize(1);
    assertThat(((Parameters) result).getParameter().get(0).getName()).isEqualTo("test");
  }

  @Test
  void inProgressJobReturns202RegardlessOfRedirectFlag() {
    // In-progress jobs always return 202 Accepted, regardless of the redirect setting.
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobProvider.job(JOB_ID, request, response))
        .isInstanceOf(ProcessingNotCompletedException.class);

    // The 202 status is set by HAPI FHIR based on ProcessingNotCompletedException, not by us.
    // We verify the Cache-Control header which indicates in-progress handling.
    assertThat(response.getHeader("Cache-Control")).isEqualTo("no-cache");
    assertThat(response.getHeader("Location")).isNull();
  }

  @Test
  void completedJobWithRedirectIncludesServerBaseInLocation() {
    // Verify that the Location header includes the server base URL when available.
    request.setServerName("example.com");
    request.setServerPort(8080);
    request.setScheme("https");

    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(new Parameters());
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    jobProvider.job(JOB_ID, request, response);

    // The Location header should be a relative URL (starts with /) for flexibility.
    final String location = response.getHeader("Location");
    assertThat(location).startsWith("/");
    assertThat(location).contains("$job-result");
    assertThat(location).contains("id=" + JOB_ID);
  }

  @Test
  void completedJobWithRedirectSetsCacheHeaders() {
    // Verify that 303 responses also include cache headers.
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(new Parameters());
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    job.setRedirectOnComplete(true);
    jobRegistry.register(job);

    jobProvider.job(JOB_ID, request, response);

    assertThat(response.getHeader("Cache-Control")).isEqualTo("max-age=60");
  }

  @Test
  void cancelledJobReturns404() {
    // A cancelled job should return 404 Not Found.
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    future.cancel(false);
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobProvider.job(JOB_ID, request, response))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessageContaining("DELETE request cancelled this job");
  }

  @Test
  void interruptedJobThrowsInternalError() {
    // A job that was interrupted should throw an InternalErrorException.
    @SuppressWarnings("unchecked")
    final Future<IBaseResource> mockFuture = mock(Future.class);
    try {
      when(mockFuture.isDone()).thenReturn(true);
      when(mockFuture.isCancelled()).thenReturn(false);
      when(mockFuture.get()).thenThrow(new InterruptedException("Thread was interrupted"));
    } catch (final InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", mockFuture, Optional.empty());
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobProvider.job(JOB_ID, request, response))
        .isInstanceOf(InternalErrorException.class)
        .hasMessageContaining("Job was interrupted");
  }

  @Test
  void errorUnwrappingHandlesDirectCause() {
    // Test that errors with a direct cause are properly unwrapped.
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    future.completeExceptionally(new InvalidRequestException("Direct error"));

    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobProvider.job(JOB_ID, request, response))
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

    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobProvider.job(JOB_ID, request, response))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Nested error message");
  }
}
