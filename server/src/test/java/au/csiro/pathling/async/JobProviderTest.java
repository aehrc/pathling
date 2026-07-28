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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AsyncConfiguration;
import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.io.JobDirectoryFileSystem;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
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
  private SparkContext sparkContext;
  private MockHttpServletRequest request;
  private MockHttpServletResponse response;

  @TempDir Path tempDir;

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

    final JobDirectoryFileSystem jobDirectoryFileSystem =
        new JobDirectoryFileSystem(tempDir.toUri(), new Configuration());
    jobProvider = new JobProvider(config, jobRegistry, jobDirectoryFileSystem, mockSpark());
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
    // Under the HL7 Asynchronous Interaction Request Pattern, completed jobs should return 303 See
    // Other with a Location header pointing to the result endpoint.
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(new Parameters());
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    job.setPattern(AsyncPattern.STANDARD_ASYNC_PATTERN);
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
    // Under the default BULK_DATA pattern, completed jobs return 200 OK with the inline result.
    final Parameters expectedResult = new Parameters();
    expectedResult
        .addParameter()
        .setName("test")
        .setValue(new org.hl7.fhir.r4.model.StringType("value"));
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(expectedResult);
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    // The pattern defaults to BULK_DATA.
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
    job.setPattern(AsyncPattern.STANDARD_ASYNC_PATTERN);
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
    job.setPattern(AsyncPattern.STANDARD_ASYNC_PATTERN);
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
    job.setPattern(AsyncPattern.STANDARD_ASYNC_PATTERN);
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

  @Test
  void deleteJobFilesRemovesDirectoryFromJobDirectoryFileSystem() throws Exception {
    // Happy path: deleteJobFiles removes the per-job directory and its contents from the warehouse
    // file system.
    final Path jobsDir = tempDir.resolve("jobs").resolve(JOB_ID);
    Files.createDirectories(jobsDir);
    Files.writeString(jobsDir.resolve("output.ndjson"), "{}");
    assertThat(Files.exists(jobsDir)).isTrue();

    jobProvider.deleteJobFiles(JOB_ID);

    assertThat(Files.exists(jobsDir)).isFalse();
  }

  @Test
  void deleteJobFilesResolvesFileSystemFromWarehouseUriNotHadoopDefault() throws Exception {
    // Regression test for issue #2612. The Hadoop default file system is deliberately configured
    // with a different scheme than the warehouse. The previous implementation resolved the default
    // file system and then operated on a warehouse path, failing with "Wrong FS" whenever the
    // warehouse used a non-default scheme such as s3a://. The fix resolves the file system from the
    // warehouse URI, so deletion succeeds regardless of the configured default.
    final Configuration hadoopConfig = new Configuration();
    hadoopConfig.set("fs.defaultFS", "hdfs://nonexistent-host:8020");
    final JobDirectoryFileSystem jobDirectoryFileSystem =
        new JobDirectoryFileSystem(tempDir.toUri(), hadoopConfig);
    final ServerConfiguration config = mock(ServerConfiguration.class);
    final JobProvider provider =
        new JobProvider(config, jobRegistry, jobDirectoryFileSystem, mockSpark());

    final Path jobsDir = tempDir.resolve("jobs").resolve(JOB_ID);
    Files.createDirectories(jobsDir);
    Files.writeString(jobsDir.resolve("output.ndjson"), "{}");
    assertThat(Files.exists(jobsDir)).isTrue();

    provider.deleteJobFiles(JOB_ID);

    assertThat(Files.exists(jobsDir)).isFalse();
  }

  @Test
  void deleteJobFilesSucceedsWhenDirectoryDoesNotExist() throws Exception {
    // An absent job directory is a normal outcome, not a failure: a job that fails removes its own
    // output as it unwinds, so a deletion that follows finds nothing. It must not be logged as a
    // problem, or the genuine failures would be lost among spurious ones.
    final ListAppender<ILoggingEvent> appender = attachJobProviderAppender();
    try {
      jobProvider.deleteJobFiles(JOB_ID);
    } finally {
      detachJobProviderAppender(appender);
    }

    assertThat(appender.list).noneMatch(JobProviderTest::isProblem);
  }

  @Test
  void deleteJobFilesTwiceDoesNotLogAFailure() throws Exception {
    // The second removal of the same job directory finds nothing left to remove, which is the
    // ordinary outcome once one party has already done the work.
    final Path jobsDir = tempDir.resolve("jobs").resolve(JOB_ID);
    Files.createDirectories(jobsDir);
    Files.writeString(jobsDir.resolve("output.ndjson"), "{}");
    jobProvider.deleteJobFiles(JOB_ID);

    final ListAppender<ILoggingEvent> appender = attachJobProviderAppender();
    try {
      jobProvider.deleteJobFiles(JOB_ID);
    } finally {
      detachJobProviderAppender(appender);
    }

    assertThat(Files.exists(jobsDir)).isFalse();
    assertThat(appender.list).noneMatch(JobProviderTest::isProblem);
  }

  // -- Deleting a job: cancelling the Spark work --

  @Test
  void deletingRunningJobCancelsItsSparkJobGroup() {
    // Nothing on the delete path used to touch Spark, so the work carried on until the server next
    // happened to observe a stage boundary for it. Signalling Spark here is what makes abandoning a
    // job actually stop it, and therefore what makes the deferred clean-up prompt.
    final Job<IBaseResource> job = registerRunningJob();

    assertThatThrownBy(() -> jobProvider.deleteJob(job.getId()))
        .isInstanceOf(ProcessingNotCompletedException.class);

    verify(sparkContext).cancelJobGroup(job.getId());
  }

  @Test
  void deletingFinishedJobAttemptsNoSparkCancellation() {
    // There is no work left to cancel, so the delete path leaves Spark alone.
    final Job<IBaseResource> job =
        jobRegistry.getOrCreate(
            new Job.JobTag() {},
            id ->
                new Job<>(
                    id,
                    "export",
                    CompletableFuture.completedFuture(new Parameters()),
                    Optional.empty()));

    assertThatThrownBy(() -> jobProvider.deleteJob(job.getId()))
        .isInstanceOf(ProcessingNotCompletedException.class);

    verify(sparkContext, never()).cancelJobGroup(anyString());
  }

  // -- Deleting a job: who removes the output directory --

  @Test
  void deletingRunningJobLeavesTheDirectoryForTheJobThread() throws Exception {
    // The work is still running, so removing its output directory now would pull the ground out
    // from under tasks that are still writing into it. The removal is left to the job's own thread,
    // and the client still receives the acceptance and sees the job leave the registry.
    final Job<IBaseResource> job = registerRunningJob();
    final Path jobsDir = createJobDirectory(job.getId());

    assertThatThrownBy(() -> jobProvider.deleteJob(job.getId()))
        .isInstanceOf(ProcessingNotCompletedException.class)
        .satisfies(
            thrown ->
                assertThat(informationalDiagnostics(thrown))
                    .containsExactly("The job and its resources will be deleted."));

    assertThat(Files.exists(jobsDir)).as("no removal is attempted while the work runs").isTrue();
    assertThat(jobRegistry.get(job.getId())).isNull();
  }

  @Test
  void deletingTerminatedJobRemovesTheDirectoryInline() throws Exception {
    // The work has already stopped, so nothing is writing and this request owns the removal.
    final Job<IBaseResource> job = registerRunningJob();
    // The job's own thread has finished unwinding without any deletion having been requested, so it
    // leaves the claim untaken.
    assertThat(job.markTerminatedAndClaim()).isFalse();
    final Path jobsDir = createJobDirectory(job.getId());

    assertThatThrownBy(() -> jobProvider.deleteJob(job.getId()))
        .isInstanceOf(ProcessingNotCompletedException.class);

    assertThat(Files.exists(jobsDir)).isFalse();
  }

  /**
   * Registers a running job through the tag-based factory, so it is present in both the id and tag
   * maps and can be removed by the delete path exactly as a real asynchronous job would be.
   *
   * @return the registered job
   */
  /**
   * Creates a Spark session whose context records job-group cancellation, and remembers the context
   * so that tests can assert against it.
   *
   * @return a mock Spark session
   */
  @Nonnull
  private SparkSession mockSpark() {
    sparkContext = mock(SparkContext.class);
    final SparkSession spark = mock(SparkSession.class);
    when(spark.sparkContext()).thenReturn(sparkContext);
    return spark;
  }

  @Nonnull
  private Job<IBaseResource> registerRunningJob() {
    return jobRegistry.getOrCreate(
        new Job.JobTag() {},
        id -> new Job<>(id, "export", new CompletableFuture<>(), Optional.empty()));
  }

  /**
   * Creates the per-job output directory with a file in it, standing in for output an operation has
   * written.
   *
   * @param jobId the identifier of the job that owns the directory
   * @return the path of the created directory
   * @throws IOException if the directory cannot be created
   */
  @Nonnull
  private Path createJobDirectory(@Nonnull final String jobId) throws IOException {
    final Path jobsDir = tempDir.resolve("jobs").resolve(jobId);
    Files.createDirectories(jobsDir);
    Files.writeString(jobsDir.resolve("output.ndjson"), "{}");
    return jobsDir;
  }

  /**
   * Extracts the diagnostics of the informational issues carried by a thrown server exception.
   *
   * @param thrown the exception to inspect
   * @return the diagnostics of each informational issue, in order
   */
  @Nonnull
  private static List<String> informationalDiagnostics(@Nonnull final Throwable thrown) {
    return issues(thrown).stream()
        .filter(issue -> issue.getSeverity() == IssueSeverity.INFORMATION)
        .map(OperationOutcomeIssueComponent::getDiagnostics)
        .toList();
  }

  /**
   * Extracts the issues carried by the {@code OperationOutcome} of a thrown server exception.
   *
   * @param thrown the exception to inspect
   * @return the issues of the attached outcome
   */
  @Nonnull
  private static List<OperationOutcomeIssueComponent> issues(@Nonnull final Throwable thrown) {
    final BaseServerResponseException exception = (BaseServerResponseException) thrown;
    return ((OperationOutcome) exception.getOperationOutcome()).getIssue();
  }

  @Nonnull
  private static ListAppender<ILoggingEvent> attachJobProviderAppender() {
    final Logger logger = (Logger) LoggerFactory.getLogger(JobProvider.class);
    final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    return appender;
  }

  private static void detachJobProviderAppender(
      @Nonnull final ListAppender<ILoggingEvent> appender) {
    ((Logger) LoggerFactory.getLogger(JobProvider.class)).detachAppender(appender);
  }

  /**
   * Tests whether a log event reports a problem, as opposed to routine progress.
   *
   * @param event the log event to test
   * @return true if the event was logged at warning level or above
   */
  private static boolean isProblem(@Nonnull final ILoggingEvent event) {
    return event.getLevel().isGreaterOrEqual(Level.WARN);
  }
}
