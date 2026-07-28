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

import static au.csiro.pathling.async.RequestTagFactoryTest.createServerConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.operations.bulkexport.ExportResultRegistry;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.aspectj.lang.ProceedingJoinPoint;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.JwtClaimAccessor;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Tests for {@link AsyncAspect}, covering job creation and reuse, the kick-off response, and the
 * clean-up the job's own thread performs as it unwinds.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
@MockitoBean(types = ExportResultRegistry.class)
@Slf4j
class AsyncAspectTest {

  @MockitoBean private ThreadPoolTaskExecutor threadPoolTaskExecutor;

  @MockitoBean private CacheableDatabase database;

  @MockitoBean private StageMap stageMap;

  @MockitoBean private ProceedingJoinPoint proceedingJoinPoint;

  @MockitoBean private JobProvider jobProvider;

  @Autowired SparkSession spark;

  private JobRegistry jobRegistry;
  private AsyncAspect asyncAspect;
  private ServerInstanceId serverInstanceId;

  private final MockHttpServletRequest servletRequest = new MockHttpServletRequest();
  private final MockHttpServletResponse servletResponse = new MockHttpServletResponse();
  private ServletRequestDetails requestDetails;

  private static final IBaseResource RESULT_RESOURCE = mock(IBaseResource.class);
  private static final AsyncSupported ASYNC_SUPPORTED = mock(AsyncSupported.class);
  private static final String FHIR_SERVER_BASE = "http://localhost:8080/fhir";

  // regular expression that matches content location header
  private static final Pattern CONTENT_LOCATION_REGEX =
      Pattern.compile("([^?]+)\\?id=([\\w\\-]{36})");

  @BeforeEach
  void setUp() throws Throwable {
    // Wire the asyncAspects and it's dependencies
    final ServerConfiguration serverConfiguration =
        createServerConfiguration(List.of("Accept", "Authorization"), List.of("Accept"));
    final RequestTagFactory requestTagFactory =
        new RequestTagFactory(database, serverConfiguration);
    jobRegistry = new JobRegistry();
    serverInstanceId = new ServerInstanceId();
    asyncAspect =
        new AsyncAspect(
            threadPoolTaskExecutor,
            requestTagFactory,
            jobRegistry,
            stageMap,
            spark,
            jobProvider,
            serverInstanceId);

    // Initialise mock request and response
    requestDetails = new ServletRequestDetails();
    requestDetails.setServletRequest(servletRequest);
    requestDetails.setServletResponse(servletResponse);
    requestDetails.setFhirServerBase(FHIR_SERVER_BASE);
    requestDetails.setCompleteUrl(FHIR_SERVER_BASE + "/Patient/$aggregate?param=value1");
    requestDetails.setOperation("$aggregate");

    final Object[] args = new Object[] {requestDetails};
    when(proceedingJoinPoint.getArgs()).thenReturn(args);
    when(proceedingJoinPoint.proceed()).thenReturn(RESULT_RESOURCE);
  }

  @Nonnull
  IBaseResource executeRequest() throws Throwable {
    return asyncAspect.maybeExecuteAsynchronously(proceedingJoinPoint, ASYNC_SUPPORTED);
  }

  void setAuthenticationPrincipal(@Nonnull final Object principal) {
    SecurityContextHolder.getContext()
        .setAuthentication(new TestingAuthenticationToken(principal, ""));
  }

  @Nonnull
  String assertExecutedAsync() {
    servletRequest.removeHeader("Prefer");
    servletRequest.addHeader("Prefer", "respond-async");
    final ProcessingNotCompletedException ex =
        assertThrows(ProcessingNotCompletedException.class, this::executeRequest);
    assertEquals(202, ex.getStatusCode());
    final String contentLocation = (String) servletResponse.getHeaderValue("Content-Location");
    assertNotNull(contentLocation);
    final Matcher matcher = CONTENT_LOCATION_REGEX.matcher(contentLocation);
    assertTrue(matcher.matches());
    final String requestUrl = matcher.group(1);
    assertEquals(FHIR_SERVER_BASE + "/$job", requestUrl);
    // return job id
    return matcher.group(2);
  }

  @Test
  void testSynchronousRequestReturnsExpectedResponse() throws Throwable {
    final IBaseResource result = executeRequest();
    assertEquals(RESULT_RESOURCE, result);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testAsyncRequestsSchedulesNewJob() {
    // setup thread pool executor to return a mock future
    final Future<IBaseResource> mockFuture = mock(Future.class);
    when(threadPoolTaskExecutor.submit(ArgumentMatchers.<Callable<IBaseResource>>any()))
        .thenReturn(mockFuture);

    // setup authentication principal
    final JwtClaimAccessor mockJwtPrincipal = mock(JwtClaimAccessor.class);
    when(mockJwtPrincipal.getSubject()).thenReturn("subject1");
    setAuthenticationPrincipal(mockJwtPrincipal);

    final String jobId = assertExecutedAsync();
    final Job<?> newJob = jobRegistry.get(jobId);
    assertNotNull(newJob);
    assertEquals(jobId, newJob.getId());
    assertEquals(mockFuture, newJob.getResult());
    assertEquals("aggregate", newJob.getOperation());
    assertEquals(Optional.of("subject1"), newJob.getOwnerId());
  }

  @Test
  void testReusesAsynJobIfOnlyWhitelistedHeadersChange() {
    setAuthenticationPrincipal("principal1");
    servletRequest.addHeader("Accept", "value1");
    assertEquals("value1", servletRequest.getHeader("Accept"));
    final String jobId1 = assertExecutedAsync();

    setAuthenticationPrincipal("principal1");
    servletRequest.removeHeader("Accept");
    servletRequest.addHeader("Accept", "value2");
    assertEquals("value2", servletRequest.getHeader("Accept"));
    final String jobId2 = assertExecutedAsync();

    assertEquals(jobId1, jobId2);
  }

  @Test
  void testCreatesNewAsyncJobWhenSalientHeaderChanges() {
    servletRequest.addHeader("Authorization", "value1");
    assertEquals("value1", servletRequest.getHeader("Authorization"));
    final String jobId1 = assertExecutedAsync();

    servletRequest.removeHeader("Authorization");
    servletRequest.addHeader("Authorization", "value2");
    assertEquals("value2", servletRequest.getHeader("Authorization"));
    final String jobId2 = assertExecutedAsync();
    assertNotEquals(jobId1, jobId2);
  }

  @Test
  void testCreatesNewAsyncJobWhenDatabaseVersionChanges() {
    when(database.getCacheKey()).thenReturn(Optional.of("key1"));
    final String jobId1 = assertExecutedAsync();
    when(database.getCacheKey()).thenReturn(Optional.of("key2"));
    final String jobId2 = assertExecutedAsync();
    assertNotEquals(jobId1, jobId2);
  }

  @Test
  void testCreatesNewAsyncJobWhenQueryStringChanges() {
    requestDetails.setCompleteUrl(FHIR_SERVER_BASE + "/Patient/$aggregate?param=value1");
    final String jobId1 = assertExecutedAsync();
    requestDetails.setCompleteUrl(FHIR_SERVER_BASE + "/Patient/$aggregate?param=value2");
    final String jobId2 = assertExecutedAsync();
    assertNotEquals(jobId1, jobId2);
  }

  @Test
  void testReusesAsyncJobWhenAuthenticationPrincipalChanges() {
    setAuthenticationPrincipal("principal1");
    final String jobId1 = assertExecutedAsync();
    setAuthenticationPrincipal("principal2");
    final String jobId2 = assertExecutedAsync();
    assertEquals(jobId1, jobId2);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testAsyncJobContextIsSetDuringAsyncExecution() throws Throwable {
    // Given: An async request that will be submitted to the executor.
    final Future<IBaseResource> mockFuture = mock(Future.class);
    final ArgumentCaptor<Callable<IBaseResource>> callableCaptor =
        ArgumentCaptor.forClass(Callable.class);
    when(threadPoolTaskExecutor.submit(callableCaptor.capture())).thenReturn(mockFuture);

    // Set up authentication principal.
    final JwtClaimAccessor mockJwtPrincipal = mock(JwtClaimAccessor.class);
    when(mockJwtPrincipal.getSubject()).thenReturn("subject1");
    setAuthenticationPrincipal(mockJwtPrincipal);

    // When: The async request is executed.
    final String jobId = assertExecutedAsync();

    // Then: Verify the callable was captured.
    verify(threadPoolTaskExecutor).submit(callableCaptor.capture());
    final Callable<IBaseResource> capturedCallable = callableCaptor.getValue();
    assertNotNull(capturedCallable);

    // When: The callable is executed (simulating async execution).
    final AtomicReference<Optional<Job<?>>> jobDuringExecution = new AtomicReference<>();
    final AtomicReference<String> jobIdDuringExecution = new AtomicReference<>();

    // Modify the ProceedingJoinPoint to capture AsyncJobContext state during execution.
    when(proceedingJoinPoint.proceed())
        .thenAnswer(
            invocation -> {
              jobDuringExecution.set(AsyncJobContext.getCurrentJob());
              if (jobDuringExecution.get().isPresent()) {
                jobIdDuringExecution.set(jobDuringExecution.get().get().getId());
              }
              return RESULT_RESOURCE;
            });

    // Set up the StageMap mock to return a non-null keySet for cleanup.
    when(stageMap.keySet())
        .thenReturn(new java.util.concurrent.ConcurrentHashMap<Integer, String>().keySet());
    when(stageMap.entrySet()).thenReturn(java.util.Collections.emptySet());

    // Execute the captured callable.
    capturedCallable.call();

    // Then: The AsyncJobContext should have had the job set during execution.
    assertTrue(
        jobDuringExecution.get().isPresent(),
        "AsyncJobContext should have the job set during async execution");
    assertEquals(
        jobId,
        jobIdDuringExecution.get(),
        "Job ID in AsyncJobContext should match the created job ID");

    // And: After execution, the context should be cleared.
    assertTrue(
        AsyncJobContext.getCurrentJob().isEmpty(),
        "AsyncJobContext should be cleared after async execution");
  }

  // -- Removal of the job's output directory as the job's thread unwinds --

  @Test
  void deletedJobHasItsFilesRemovedAsTheTaskUnwinds() throws Throwable {
    // A client deleted the job while its work was still running, so the request left the removal to
    // this thread. As the task unwinds it takes the claim and removes the output.
    final Callable<IBaseResource> task = captureSubmittedTask();
    final String jobId = assertExecutedAsync();
    final Job<?> job = jobRegistry.get(jobId);
    assertNotNull(job);
    // The delete request marked the job but did not claim, because the work had not terminated.
    assertFalse(job.markDeletedAndClaim());

    task.call();

    verify(jobProvider).deleteJobFiles(jobId);
  }

  @Test
  void completedJobThatWasNeverDeletedKeepsItsFiles() throws Throwable {
    // Nothing was deleted, so the output must survive for the client to download.
    final Callable<IBaseResource> task = captureSubmittedTask();
    final String jobId = assertExecutedAsync();

    task.call();

    verify(jobProvider, never()).deleteJobFiles(jobId);
  }

  @Test
  void failedJobHasItsFilesRemovedEvenWhenNeverDeleted() throws Throwable {
    // A job that fails removes its own partial output as it unwinds, whether or not a client asked
    // for it to be deleted. This is existing behaviour, retained now that the removal has moved to
    // the single site in the finally block.
    final Callable<IBaseResource> task = captureSubmittedTask();
    final String jobId = assertExecutedAsync();
    when(proceedingJoinPoint.proceed()).thenThrow(new IllegalArgumentException("failed"));

    assertThrows(IllegalStateException.class, task::call);

    verify(jobProvider).deleteJobFiles(jobId);
  }

  @Test
  void removalUsesTheCapturedJobRatherThanTheRegistry() throws Throwable {
    // The delete request lands while the work is running, so by the time the task unwinds the job
    // is
    // no longer in the registry. A lookup at that point would find nothing and skip the removal, so
    // the task must use the reference it captured when it started.
    final Callable<IBaseResource> task = captureSubmittedTask();
    final String jobId = assertExecutedAsync();
    final Job<?> job = jobRegistry.get(jobId);
    assertNotNull(job);
    when(proceedingJoinPoint.proceed())
        .thenAnswer(
            invocation -> {
              // Stand in for the delete request arriving mid-work: the job is marked, the claim is
              // left to this thread, and the job leaves the registry.
              assertFalse(job.markDeletedAndClaim());
              assertTrue(jobRegistry.remove(job));
              return RESULT_RESOURCE;
            });

    task.call();

    assertNull(jobRegistry.get(jobId));
    verify(jobProvider).deleteJobFiles(jobId);
  }

  @Test
  void removalFailureOnTheJobThreadIsNotPropagated() throws Throwable {
    // Throwing out of the finally block would replace the job's own exception with an incidental
    // one, so a failed removal is reported and swallowed instead.
    final Callable<IBaseResource> task = captureSubmittedTask();
    final String jobId = assertExecutedAsync();
    final Job<?> job = jobRegistry.get(jobId);
    assertNotNull(job);
    assertFalse(job.markDeletedAndClaim());
    doThrow(new IOException("warehouse is unavailable")).when(jobProvider).deleteJobFiles(jobId);

    final ListAppender<ILoggingEvent> appender = attachAppender(JobProvider.class);
    try {
      assertDoesNotThrow(task::call);
    } finally {
      detachAppender(JobProvider.class, appender);
    }

    assertTrue(
        appender.list.stream().anyMatch(event -> event.getLevel() == Level.ERROR),
        "the failed removal should be logged at error level");
  }

  /**
   * Arranges for the executor to capture the submitted task rather than run it, and returns a
   * handle that invokes it on the calling thread. Stage map interactions are stubbed for the
   * clean-up that the task performs as it unwinds.
   *
   * @return the task that the aspect submits for the next asynchronous request
   */
  @Nonnull
  @SuppressWarnings("unchecked")
  private Callable<IBaseResource> captureSubmittedTask() {
    final Future<IBaseResource> mockFuture = mock(Future.class);
    final ArgumentCaptor<Callable<IBaseResource>> captor = ArgumentCaptor.forClass(Callable.class);
    when(threadPoolTaskExecutor.submit(captor.capture())).thenReturn(mockFuture);
    when(stageMap.entrySet()).thenReturn(java.util.Collections.emptySet());
    when(stageMap.keySet())
        .thenReturn(new java.util.concurrent.ConcurrentHashMap<Integer, String>().keySet());
    return () -> captor.getValue().call();
  }

  @Nonnull
  private static ListAppender<ILoggingEvent> attachAppender(@Nonnull final Class<?> loggerClass) {
    final Logger logger = (Logger) LoggerFactory.getLogger(loggerClass);
    final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    return appender;
  }

  private static void detachAppender(
      @Nonnull final Class<?> loggerClass, @Nonnull final ListAppender<ILoggingEvent> appender) {
    ((Logger) LoggerFactory.getLogger(loggerClass)).detachAppender(appender);
  }

  @Test
  void asyncResponseSetsInstanceSpecificEtag() {
    // Async responses should set an ETag that includes the server instance ID and a hash of the
    // job ID. This ensures cached 202 responses are invalidated after a server restart.
    assertExecutedAsync();

    // Verify the ETag header is set with the expected format: W/"~{instanceId}.{hash}".
    final String etag = servletResponse.getHeader("ETag");
    assertNotNull(etag, "ETag header should be set on async response");
    assertTrue(etag.startsWith("W/\"~"), "ETag should have async prefix (~)");
    assertTrue(etag.contains(serverInstanceId.getId()), "ETag should contain server instance ID");
    assertTrue(etag.contains("."), "ETag should contain dot separator");
    // Verify the hash is 8 hex characters. The format after W/" is: ~{instanceId}.{hash}.
    final String etagContent = etag.substring(3, etag.length() - 1); // Strip W/" and trailing ".
    final String[] parts = etagContent.split("\\.");
    assertEquals(2, parts.length, "ETag should have exactly two parts separated by dot");
    assertTrue(parts[0].startsWith("~"), "First part should start with ~");
    assertEquals(9, parts[0].length(), "First part should be 9 chars (~ + 8-char instance ID)");
    assertEquals(8, parts[1].length(), "Hash should be 8 characters");
    assertTrue(parts[1].matches("[0-9a-f]{8}"), "Hash should be 8 hex characters");
  }
}
