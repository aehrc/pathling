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

package au.csiro.pathling.operations.bulksubmit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.config.BulkSubmitConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.operations.bulkexport.ExportResultRegistry;
import au.csiro.pathling.operations.bulkimport.ImportExecutor;
import au.csiro.pathling.operations.bulkimport.ImportRequest;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for BulkSubmitExecutor.
 *
 * @author John Grimes
 */
class BulkSubmitExecutorTest {

  private static final String SUBMITTER_SYSTEM = "https://example.org/submitters";
  private static final String SUBMITTER_VALUE = "test-submitter";
  private static final String SUBMISSION_ID = "test-submission-123";
  private static final String MANIFEST_JOB_ID = "manifest-job-456";
  private static final String FHIR_SERVER_BASE = "http://localhost:8080/fhir";

  private static WireMockServer wireMockServer;

  @TempDir Path tempDir;

  private ImportExecutor importExecutor;
  private SubmissionRegistry submissionRegistry;
  private ExportResultRegistry exportResultRegistry;
  private ServerConfiguration serverConfiguration;
  private BulkSubmitResultBuilder resultBuilder;
  private JobRegistry jobRegistry;
  private SparkSession sparkSession;
  private SparkContext sparkContext;
  private FhirContext fhirContext;
  private BulkSubmitAuthProvider authProvider;

  private BulkSubmitExecutor executor;
  private SubmitterIdentifier submitterIdentifier;

  @BeforeAll
  static void setupWireMock() {
    wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMockServer.start();
  }

  @AfterAll
  static void tearDownWireMock() {
    if (wireMockServer != null && wireMockServer.isRunning()) {
      wireMockServer.stop();
    }
  }

  @BeforeEach
  void setUp() {
    importExecutor = mock(ImportExecutor.class);
    submissionRegistry = mock(SubmissionRegistry.class);
    exportResultRegistry = mock(ExportResultRegistry.class);
    serverConfiguration = mock(ServerConfiguration.class);
    resultBuilder = mock(BulkSubmitResultBuilder.class);
    jobRegistry = mock(JobRegistry.class);
    sparkSession = mock(SparkSession.class);
    sparkContext = mock(SparkContext.class);
    fhirContext = FhirEncoders.contextFor(FhirVersionEnum.R4);
    authProvider = mock(BulkSubmitAuthProvider.class);

    when(sparkSession.sparkContext()).thenReturn(sparkContext);

    // Configure result builder to return a simple Parameters resource.
    when(resultBuilder.buildStatusManifest(any(), anyString(), anyString()))
        .thenReturn(new Parameters());

    final String databasePath = "file://" + tempDir.toAbsolutePath();
    executor =
        new BulkSubmitExecutor(
            importExecutor,
            submissionRegistry,
            exportResultRegistry,
            serverConfiguration,
            resultBuilder,
            jobRegistry,
            sparkSession,
            databasePath,
            fhirContext,
            authProvider);

    submitterIdentifier = new SubmitterIdentifier(SUBMITTER_SYSTEM, SUBMITTER_VALUE);

    // Reset WireMock stubs before each test.
    wireMockServer.resetAll();
  }

  @AfterEach
  void cleanup() {
    // Clean up temp directory.
  }

  // ========================================
  // downloadManifestJob Tests
  // ========================================

  @Test
  @DisplayName("downloadManifestJob creates and registers job when async enabled")
  @SuppressWarnings("unchecked")
  void downloadManifestJobCreatesAndRegistersJobWhenAsyncEnabled() {
    // Given: a submission and manifest job with async enabled.
    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupSuccessfulManifestStubs();

    // Capture the job registered.
    final ArgumentCaptor<Job<?>> jobCaptor = ArgumentCaptor.forClass(Job.class);

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should register a Job in the JobRegistry.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(jobRegistry).register(jobCaptor.capture());
              final Job<?> registeredJob = jobCaptor.getValue();
              assertThat(registeredJob).isNotNull();
              assertThat(registeredJob.getOwnerId()).isEqualTo(Optional.empty());
            });
  }

  @Test
  @DisplayName("downloadManifestJob skips job registration when JobRegistry null")
  void downloadManifestJobSkipsJobRegistrationWhenJobRegistryNull() {
    // Given: executor without JobRegistry.
    final String databasePath = "file://" + tempDir.toAbsolutePath();
    final BulkSubmitExecutor executorNoAsync =
        new BulkSubmitExecutor(
            importExecutor,
            submissionRegistry,
            exportResultRegistry,
            serverConfiguration,
            resultBuilder,
            null, // no JobRegistry
            sparkSession,
            databasePath,
            fhirContext,
            authProvider);

    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupSuccessfulManifestStubs();

    // When: calling downloadManifestJob.
    executorNoAsync.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should not register any job, but state should still be updated.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(jobRegistry, never()).register(any());
              // State updates happen at least twice (processing and final state).
              verify(submissionRegistry, times(2))
                  .updateManifestJob(
                      eq(submitterIdentifier), eq(SUBMISSION_ID), eq(MANIFEST_JOB_ID), any());
            });
  }

  @Test
  @DisplayName("downloadManifestJob skips job registration when SparkSession null")
  void downloadManifestJobSkipsJobRegistrationWhenSparkSessionNull() {
    // Given: executor without SparkSession.
    final String databasePath = "file://" + tempDir.toAbsolutePath();
    final BulkSubmitExecutor executorNoSpark =
        new BulkSubmitExecutor(
            importExecutor,
            submissionRegistry,
            exportResultRegistry,
            serverConfiguration,
            resultBuilder,
            jobRegistry,
            null, // no SparkSession
            databasePath,
            fhirContext,
            authProvider);

    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupSuccessfulManifestStubs();

    // When: calling downloadManifestJob.
    executorNoSpark.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should not register any job.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> verify(jobRegistry, never()).register(any()));
  }

  @Test
  @DisplayName("downloadManifestJob sets Spark job group for progress tracking")
  void downloadManifestJobSetsSparkJobGroupForProgressTracking() {
    // Given: a submission and manifest job.
    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupSuccessfulManifestStubs();

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should set and clear Spark job group.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(sparkContext).setJobGroup(anyString(), anyString(), eq(true));
              verify(sparkContext).clearJobGroup();
            });
  }

  @Test
  @DisplayName("downloadManifestJob updates state to PROCESSING on start")
  void downloadManifestJobUpdatesStateToProcessingOnStart() {
    // Given: a submission and manifest job.
    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupSuccessfulManifestStubs();

    // Capture state updates.
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<UnaryOperator<ManifestJob>> updateCaptor =
        ArgumentCaptor.forClass(UnaryOperator.class);

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should update state to PROCESSING (first call after job ID update).
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(submissionRegistry, times(3))
                  .updateManifestJob(
                      eq(submitterIdentifier),
                      eq(SUBMISSION_ID),
                      eq(MANIFEST_JOB_ID),
                      updateCaptor.capture());

              // Second update should be to PROCESSING state.
              final UnaryOperator<ManifestJob> secondUpdate = updateCaptor.getAllValues().get(1);
              final ManifestJob updatedJob = secondUpdate.apply(manifestJob);
              assertThat(updatedJob.state()).isEqualTo(ManifestJobState.PROCESSING);
            });
  }

  @Test
  @DisplayName("downloadManifestJob updates state to DOWNLOADED on success")
  void downloadManifestJobUpdatesStateToDownloadedOnSuccess() {
    // Given: a submission and manifest job.
    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupSuccessfulManifestStubs();

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<UnaryOperator<ManifestJob>> updateCaptor =
        ArgumentCaptor.forClass(UnaryOperator.class);

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should update state to DOWNLOADED (third call).
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(submissionRegistry, times(3))
                  .updateManifestJob(
                      eq(submitterIdentifier),
                      eq(SUBMISSION_ID),
                      eq(MANIFEST_JOB_ID),
                      updateCaptor.capture());

              // Third update should be to DOWNLOADED state with files.
              final UnaryOperator<ManifestJob> thirdUpdate = updateCaptor.getAllValues().get(2);
              final ManifestJob updatedJob = thirdUpdate.apply(manifestJob);
              assertThat(updatedJob.state()).isEqualTo(ManifestJobState.DOWNLOADED);
              assertThat(updatedJob.downloadedFiles()).isNotEmpty();
            });
  }

  @Test
  @DisplayName("downloadManifestJob updates state to FAILED on error")
  void downloadManifestJobUpdatesStateToFailedOnError() {
    // Given: a submission and manifest job with failing endpoint.
    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupFailingManifestStub();

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<UnaryOperator<ManifestJob>> updateCaptor =
        ArgumentCaptor.forClass(UnaryOperator.class);

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should update state to FAILED.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(submissionRegistry, times(3))
                  .updateManifestJob(
                      eq(submitterIdentifier),
                      eq(SUBMISSION_ID),
                      eq(MANIFEST_JOB_ID),
                      updateCaptor.capture());

              // Third update should be to FAILED state with error.
              final UnaryOperator<ManifestJob> thirdUpdate = updateCaptor.getAllValues().get(2);
              final ManifestJob updatedJob = thirdUpdate.apply(manifestJob);
              assertThat(updatedJob.state()).isEqualTo(ManifestJobState.FAILED);
              assertThat(updatedJob.errorMessage()).isNotNull();
            });
  }

  @Test
  @DisplayName("downloadManifestJob writes error file on failure")
  void downloadManifestJobWritesErrorFileOnFailure() {
    // Given: a submission and manifest job with failing endpoint.
    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupFailingManifestStub();

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<UnaryOperator<ManifestJob>> updateCaptor =
        ArgumentCaptor.forClass(UnaryOperator.class);

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should write an error file.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(submissionRegistry, times(3))
                  .updateManifestJob(
                      eq(submitterIdentifier),
                      eq(SUBMISSION_ID),
                      eq(MANIFEST_JOB_ID),
                      updateCaptor.capture());

              final UnaryOperator<ManifestJob> thirdUpdate = updateCaptor.getAllValues().get(2);
              final ManifestJob updatedJob = thirdUpdate.apply(manifestJob);
              assertThat(updatedJob.errorFileName()).isNotNull();
              assertThat(updatedJob.errorFileName()).contains("OperationOutcome");

              // Verify error file exists.
              final Path errorPath =
                  tempDir
                      .resolve("jobs")
                      .resolve(SUBMISSION_ID)
                      .resolve(updatedJob.errorFileName());
              assertThat(Files.exists(errorPath)).isTrue();
            });
  }

  @Test
  @DisplayName("downloadManifestJob registers submission in export registry")
  void downloadManifestJobRegistersSubmissionInExportRegistry() {
    // Given: a submission and manifest job.
    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupSuccessfulManifestStubs();

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should register in export registry.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> verify(exportResultRegistry).put(eq(SUBMISSION_ID), any()));
  }

  @Test
  @DisplayName("downloadManifestJob acquires OAuth token when fhirBaseUrl provided")
  void downloadManifestJobAcquiresOAuthTokenWhenFhirBaseUrlProvided() throws Exception {
    // Given: a submission and manifest job with fhirBaseUrl.
    final Submission submission = createTestSubmission();
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port() + "/fhir";
    final ManifestJob manifestJob = createTestManifestJobWithFhirBaseUrl(fhirBaseUrl);
    setupSuccessfulManifestStubs();

    when(authProvider.acquireToken(any(), anyString(), any()))
        .thenReturn(Optional.of("test-token"));

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should attempt to acquire token.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                verify(authProvider).acquireToken(eq(submitterIdentifier), eq(fhirBaseUrl), any()));
  }

  @Test
  @DisplayName("downloadManifestJob continues without auth when token acquisition fails")
  void downloadManifestJobContinuesWithoutAuthWhenTokenAcquisitionFails() throws Exception {
    // Given: a submission and manifest job where auth fails.
    final Submission submission = createTestSubmission();
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port() + "/fhir";
    final ManifestJob manifestJob = createTestManifestJobWithFhirBaseUrl(fhirBaseUrl);
    setupSuccessfulManifestStubs();

    when(authProvider.acquireToken(any(), anyString(), any()))
        .thenThrow(new IOException("Auth failed"));

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<UnaryOperator<ManifestJob>> updateCaptor =
        ArgumentCaptor.forClass(UnaryOperator.class);

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should still succeed with DOWNLOADED state.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(submissionRegistry, times(3))
                  .updateManifestJob(
                      eq(submitterIdentifier),
                      eq(SUBMISSION_ID),
                      eq(MANIFEST_JOB_ID),
                      updateCaptor.capture());

              final UnaryOperator<ManifestJob> thirdUpdate = updateCaptor.getAllValues().get(2);
              final ManifestJob updatedJob = thirdUpdate.apply(manifestJob);
              assertThat(updatedJob.state()).isEqualTo(ManifestJobState.DOWNLOADED);
            });
  }

  @Test
  @DisplayName("downloadManifestJob uses token for files when requiresAccessToken true")
  void downloadManifestJobUsesTokenForFilesWhenRequiresAccessTokenTrue() throws Exception {
    // Given: a manifest that requires access token.
    final Submission submission = createTestSubmission();
    final String fhirBaseUrl = "http://localhost:" + wireMockServer.port() + "/fhir";
    final ManifestJob manifestJob = createTestManifestJobWithFhirBaseUrl(fhirBaseUrl);
    setupManifestWithRequiresAccessToken();

    when(authProvider.acquireToken(any(), anyString(), any()))
        .thenReturn(Optional.of("test-token"));

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should include Authorization header in file requests.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                wireMockServer.verify(
                    getRequestedFor(urlEqualTo("/data/Patient.ndjson"))
                        .withHeader("Authorization", equalTo("Bearer test-token"))));
  }

  @Test
  @DisplayName("downloadManifestJob handles manifest with no output array")
  void downloadManifestJobHandlesManifestWithNoOutput() {
    // Given: a manifest with no output.
    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupEmptyManifestStub();

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<UnaryOperator<ManifestJob>> updateCaptor =
        ArgumentCaptor.forClass(UnaryOperator.class);

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should fail with appropriate error.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(submissionRegistry, times(3))
                  .updateManifestJob(
                      eq(submitterIdentifier),
                      eq(SUBMISSION_ID),
                      eq(MANIFEST_JOB_ID),
                      updateCaptor.capture());

              final UnaryOperator<ManifestJob> thirdUpdate = updateCaptor.getAllValues().get(2);
              final ManifestJob updatedJob = thirdUpdate.apply(manifestJob);
              assertThat(updatedJob.state()).isEqualTo(ManifestJobState.FAILED);
              assertThat(updatedJob.errorMessage()).contains("No files found");
            });
  }

  @Test
  @DisplayName("downloadManifestJob handles manifest with missing type field")
  void downloadManifestJobHandlesManifestWithMissingType() {
    // Given: a manifest with missing type field.
    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupManifestWithMissingTypeStub();

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<UnaryOperator<ManifestJob>> updateCaptor =
        ArgumentCaptor.forClass(UnaryOperator.class);

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should fail with appropriate error.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(submissionRegistry, times(3))
                  .updateManifestJob(
                      eq(submitterIdentifier),
                      eq(SUBMISSION_ID),
                      eq(MANIFEST_JOB_ID),
                      updateCaptor.capture());

              final UnaryOperator<ManifestJob> thirdUpdate = updateCaptor.getAllValues().get(2);
              final ManifestJob updatedJob = thirdUpdate.apply(manifestJob);
              assertThat(updatedJob.state()).isEqualTo(ManifestJobState.FAILED);
              assertThat(updatedJob.errorMessage()).contains("missing 'type' field");
            });
  }

  @Test
  @DisplayName("downloadManifestJob clears Spark job group after completion")
  void downloadManifestJobClearsSparkJobGroupAfterCompletion() {
    // Given: a submission and manifest job.
    final Submission submission = createTestSubmission();
    final ManifestJob manifestJob = createTestManifestJob();
    setupSuccessfulManifestStubs();

    // When: calling downloadManifestJob.
    executor.downloadManifestJob(submission, manifestJob, List.of(), FHIR_SERVER_BASE);

    // Then: should clear job group in finally block.
    await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(sparkContext).clearJobGroup());
  }

  // ========================================
  // abortSubmission Tests
  // ========================================

  @Test
  @DisplayName("abortSubmission cancels running async jobs")
  void abortSubmissionCancelsRunningAsyncJobs() {
    // Given: a submission with a running async job.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withJobId("async-job-123");
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    @SuppressWarnings("rawtypes")
    final CompletableFuture future = new CompletableFuture<>();
    @SuppressWarnings("unchecked")
    final Job<?> asyncJob = mock(Job.class);
    doReturn(future).when(asyncJob).getResult();
    doReturn(asyncJob).when(jobRegistry).get("async-job-123");

    // When: calling abortSubmission.
    executor.abortSubmission(submission);

    // Then: should cancel the async job.
    assertThat(future.isCancelled()).isTrue();
  }

  @Test
  @DisplayName("abortSubmission skips cancelling completed jobs")
  void abortSubmissionSkipsCancellingCompletedJobs() {
    // Given: a submission with a completed async job.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withJobId("async-job-123");
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    @SuppressWarnings("rawtypes")
    final CompletableFuture future = CompletableFuture.completedFuture(new Parameters());
    @SuppressWarnings("unchecked")
    final Job<?> asyncJob = mock(Job.class);
    doReturn(future).when(asyncJob).getResult();
    doReturn(asyncJob).when(jobRegistry).get("async-job-123");

    // When: calling abortSubmission.
    executor.abortSubmission(submission);

    // Then: should not cancel already completed job.
    assertThat(future.isCancelled()).isFalse();
  }

  @Test
  @DisplayName("abortSubmission removes from export result registry")
  void abortSubmissionRemovesFromExportResultRegistry() {
    // Given: a submission.
    final Submission submission = createTestSubmission();

    // When: calling abortSubmission.
    executor.abortSubmission(submission);

    // Then: should remove from export registry.
    verify(exportResultRegistry).remove(SUBMISSION_ID);
  }

  @Test
  @DisplayName("abortSubmission updates non-terminal manifest jobs to ABORTED")
  void abortSubmissionUpdatesNonTerminalManifestJobsToAborted() {
    // Given: a submission with a PROCESSING manifest job.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withState(ManifestJobState.PROCESSING);
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<UnaryOperator<ManifestJob>> updateCaptor =
        ArgumentCaptor.forClass(UnaryOperator.class);

    // When: calling abortSubmission.
    executor.abortSubmission(submission);

    // Then: should update to ABORTED state.
    verify(submissionRegistry)
        .updateManifestJob(
            eq(submitterIdentifier),
            eq(SUBMISSION_ID),
            eq(MANIFEST_JOB_ID),
            updateCaptor.capture());

    final ManifestJob updatedJob = updateCaptor.getValue().apply(manifestJob);
    assertThat(updatedJob.state()).isEqualTo(ManifestJobState.ABORTED);
  }

  @Test
  @DisplayName("abortSubmission skips terminal manifest jobs")
  void abortSubmissionSkipsTerminalManifestJobs() {
    // Given: a submission with a COMPLETED manifest job.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withState(ManifestJobState.COMPLETED);
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    // When: calling abortSubmission.
    executor.abortSubmission(submission);

    // Then: should not update the manifest job (it's terminal).
    verify(submissionRegistry, never()).updateManifestJob(any(), anyString(), anyString(), any());
  }

  @Test
  @DisplayName("abortSubmission handles submission with no manifest jobs")
  void abortSubmissionHandlesSubmissionWithNoManifestJobs() {
    // Given: a submission with no manifest jobs.
    final Submission submission = createTestSubmission();

    // When: calling abortSubmission.
    executor.abortSubmission(submission);

    // Then: should complete without error.
    verify(exportResultRegistry).remove(SUBMISSION_ID);
    verify(submissionRegistry, never()).updateManifestJob(any(), anyString(), anyString(), any());
  }

  @Test
  @DisplayName("abortSubmission handles null JobRegistry")
  void abortSubmissionHandlesNullJobRegistry() {
    // Given: executor without JobRegistry.
    final String databasePath = "file://" + tempDir.toAbsolutePath();
    final BulkSubmitExecutor executorNoAsync =
        new BulkSubmitExecutor(
            importExecutor,
            submissionRegistry,
            exportResultRegistry,
            serverConfiguration,
            resultBuilder,
            null, // no JobRegistry
            sparkSession,
            databasePath,
            fhirContext,
            authProvider);

    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withJobId("async-job-123")
            .withState(ManifestJobState.PROCESSING);
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    // When: calling abortSubmission.
    executorNoAsync.abortSubmission(submission);

    // Then: should complete without trying to access JobRegistry.
    verify(exportResultRegistry).remove(SUBMISSION_ID);
    verify(submissionRegistry)
        .updateManifestJob(eq(submitterIdentifier), eq(SUBMISSION_ID), eq(MANIFEST_JOB_ID), any());
  }

  // ========================================
  // abortManifestJob Tests
  // ========================================

  @Test
  @DisplayName("abortManifestJob cancels running async job")
  void abortManifestJobCancelsRunningAsyncJob() {
    // Given: a manifest job with a running async job.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withJobId("async-job-123")
            .withState(ManifestJobState.PROCESSING);
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    @SuppressWarnings("rawtypes")
    final CompletableFuture future = new CompletableFuture<>();
    @SuppressWarnings("unchecked")
    final Job<?> asyncJob = mock(Job.class);
    doReturn(future).when(asyncJob).getResult();
    doReturn(asyncJob).when(jobRegistry).get("async-job-123");

    // When: calling abortManifestJob.
    executor.abortManifestJob(submission, manifestJob);

    // Then: should cancel the async job.
    assertThat(future.isCancelled()).isTrue();
  }

  @Test
  @DisplayName("abortManifestJob skips when jobId null")
  void abortManifestJobSkipsWhenJobIdNull() {
    // Given: a manifest job without a job ID.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null);
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    // When: calling abortManifestJob.
    executor.abortManifestJob(submission, manifestJob);

    // Then: should not try to get job from registry.
    verify(jobRegistry, never()).get(anyString());
  }

  @Test
  @DisplayName("abortManifestJob updates state to ABORTED")
  void abortManifestJobUpdatesStateToAborted() {
    // Given: a manifest job.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withState(ManifestJobState.PROCESSING);
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<UnaryOperator<ManifestJob>> updateCaptor =
        ArgumentCaptor.forClass(UnaryOperator.class);

    // When: calling abortManifestJob.
    executor.abortManifestJob(submission, manifestJob);

    // Then: should update state to ABORTED.
    verify(submissionRegistry)
        .updateManifestJob(
            eq(submitterIdentifier),
            eq(SUBMISSION_ID),
            eq(MANIFEST_JOB_ID),
            updateCaptor.capture());

    final ManifestJob updatedJob = updateCaptor.getValue().apply(manifestJob);
    assertThat(updatedJob.state()).isEqualTo(ManifestJobState.ABORTED);
  }

  // ========================================
  // importSubmission Tests
  // ========================================

  @Test
  @DisplayName("importSubmission executes import with downloaded files")
  void importSubmissionExecutesImportWithDownloadedFiles() {
    // Given: a submission with downloaded files.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withState(ManifestJobState.DOWNLOADED)
            .withDownloadedFiles(
                List.of(
                    new DownloadedFile(
                        "Patient",
                        "Patient.test-1.ndjson",
                        "file:///tmp/Patient.test-1.ndjson",
                        "http://example.org/manifest")));
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    final BulkSubmitConfiguration bulkSubmitConfig = mock(BulkSubmitConfiguration.class);
    when(bulkSubmitConfig.getAllowableSources()).thenReturn(List.of("http://localhost"));
    when(serverConfiguration.getBulkSubmit()).thenReturn(bulkSubmitConfig);

    final ArgumentCaptor<ImportRequest> importCaptor = ArgumentCaptor.forClass(ImportRequest.class);

    // When: calling importSubmission.
    executor.importSubmission(submission);

    // Then: should call import executor.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(importExecutor).execute(importCaptor.capture(), eq(SUBMISSION_ID), any());
              assertThat(importCaptor.getValue().input()).containsKey("Patient");
            });
  }

  @Test
  @DisplayName("importSubmission marks downloaded jobs as completed")
  void importSubmissionMarksDownloadedJobsAsCompleted() {
    // Given: a submission with downloaded files.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withState(ManifestJobState.DOWNLOADED)
            .withDownloadedFiles(
                List.of(
                    new DownloadedFile(
                        "Patient",
                        "Patient.test-1.ndjson",
                        "file:///tmp/Patient.test-1.ndjson",
                        "http://example.org/manifest")));
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    when(serverConfiguration.getBulkSubmit()).thenReturn(null);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<UnaryOperator<ManifestJob>> updateCaptor =
        ArgumentCaptor.forClass(UnaryOperator.class);

    // When: calling importSubmission.
    executor.importSubmission(submission);

    // Then: should update state to COMPLETED.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(submissionRegistry)
                  .updateManifestJob(
                      eq(submitterIdentifier),
                      eq(SUBMISSION_ID),
                      eq(MANIFEST_JOB_ID),
                      updateCaptor.capture());

              final ManifestJob updatedJob = updateCaptor.getValue().apply(manifestJob);
              assertThat(updatedJob.state()).isEqualTo(ManifestJobState.COMPLETED);
            });
  }

  @Test
  @DisplayName("importSubmission marks jobs as failed on import error")
  void importSubmissionMarksDownloadedJobsAsFailedOnImportError() {
    // Given: a submission with downloaded files and failing import.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withState(ManifestJobState.DOWNLOADED)
            .withDownloadedFiles(
                List.of(
                    new DownloadedFile(
                        "Patient",
                        "Patient.test-1.ndjson",
                        "file:///tmp/Patient.test-1.ndjson",
                        "http://example.org/manifest")));
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    when(serverConfiguration.getBulkSubmit()).thenReturn(null);
    doThrow(new RuntimeException("Import failed"))
        .when(importExecutor)
        .execute(any(), any(), any());

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<UnaryOperator<ManifestJob>> updateCaptor =
        ArgumentCaptor.forClass(UnaryOperator.class);

    // When: calling importSubmission.
    executor.importSubmission(submission);

    // Then: should update state to FAILED.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              verify(submissionRegistry)
                  .updateManifestJob(
                      eq(submitterIdentifier),
                      eq(SUBMISSION_ID),
                      eq(MANIFEST_JOB_ID),
                      updateCaptor.capture());

              final ManifestJob updatedJob = updateCaptor.getValue().apply(manifestJob);
              assertThat(updatedJob.state()).isEqualTo(ManifestJobState.FAILED);
              assertThat(updatedJob.errorMessage()).contains("Import failed");
            });
  }

  @Test
  @DisplayName("importSubmission skips manifest jobs with null downloaded files")
  void importSubmissionSkipsManifestJobsWithNullDownloadedFiles() {
    // Given: a submission with no downloaded files.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withState(ManifestJobState.DOWNLOADED);
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    // When: calling importSubmission.
    executor.importSubmission(submission);

    // Then: should not call import executor (no files to import).
    await()
        .during(Duration.ofMillis(500))
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> verify(importExecutor, never()).execute(any(), any(), any()));
  }

  @Test
  @DisplayName("importSubmission skips jobs not in DOWNLOADED state")
  void importSubmissionSkipsJobsNotInDownloadedState() {
    // Given: a submission with a PROCESSING job (not DOWNLOADED).
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withState(ManifestJobState.PROCESSING)
            .withDownloadedFiles(
                List.of(
                    new DownloadedFile(
                        "Patient",
                        "Patient.test-1.ndjson",
                        "file:///tmp/Patient.test-1.ndjson",
                        "http://example.org/manifest")));
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    when(serverConfiguration.getBulkSubmit()).thenReturn(null);

    // When: calling importSubmission.
    executor.importSubmission(submission);

    // Then: should call import executor (files are collected regardless of state).
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> verify(importExecutor).execute(any(), any(), any()));

    // But should NOT update the PROCESSING job to COMPLETED (only DOWNLOADED jobs are updated).
    verify(submissionRegistry, never()).updateManifestJob(any(), anyString(), anyString(), any());
  }

  @Test
  @DisplayName("importSubmission logs warning when no files to import")
  void importSubmissionLogsWarningWhenNoFilesToImport() {
    // Given: a submission with no files.
    final Submission submission = createTestSubmission();

    // When: calling importSubmission.
    executor.importSubmission(submission);

    // Then: should not call import executor.
    await()
        .during(Duration.ofMillis(500))
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> verify(importExecutor, never()).execute(any(), any(), any()));
  }

  @Test
  @DisplayName("importSubmission handles null bulk submit configuration")
  void importSubmissionHandlesNullBulkSubmitConfiguration() {
    // Given: a submission with downloaded files and null config.
    final ManifestJob manifestJob =
        ManifestJob.createPending(MANIFEST_JOB_ID, "http://example.org/manifest", null, null)
            .withState(ManifestJobState.DOWNLOADED)
            .withDownloadedFiles(
                List.of(
                    new DownloadedFile(
                        "Patient",
                        "Patient.test-1.ndjson",
                        "file:///tmp/Patient.test-1.ndjson",
                        "http://example.org/manifest")));
    final Submission submission = createSubmissionWithManifestJob(manifestJob);

    when(serverConfiguration.getBulkSubmit()).thenReturn(null);

    // When: calling importSubmission.
    executor.importSubmission(submission);

    // Then: should call import with empty allowable sources.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> verify(importExecutor).execute(any(), eq(SUBMISSION_ID), eq(List.of())));
  }

  // ========================================
  // Helper Methods
  // ========================================

  private Submission createTestSubmission() {
    return Submission.createPending(SUBMISSION_ID, submitterIdentifier, Optional.empty());
  }

  private Submission createSubmissionWithManifestJob(final ManifestJob manifestJob) {
    return new Submission(
        SUBMISSION_ID,
        submitterIdentifier,
        SubmissionState.PROCESSING,
        new ArrayList<>(List.of(manifestJob)),
        "2024-01-01T00:00:00Z",
        null,
        null,
        null,
        null);
  }

  private ManifestJob createTestManifestJob() {
    final String manifestUrl = "http://localhost:" + wireMockServer.port() + "/manifest.json";
    return ManifestJob.createPending(MANIFEST_JOB_ID, manifestUrl, null, null);
  }

  private ManifestJob createTestManifestJobWithFhirBaseUrl(final String fhirBaseUrl) {
    final String manifestUrl = "http://localhost:" + wireMockServer.port() + "/manifest.json";
    return ManifestJob.createPending(MANIFEST_JOB_ID, manifestUrl, fhirBaseUrl, null);
  }

  private void setupSuccessfulManifestStubs() {
    final String baseUrl = "http://localhost:" + wireMockServer.port();

    // Stub manifest endpoint.
    final String manifest =
        String.format(
            """
            {
              "transactionTime": "2025-11-28T00:00:00Z",
              "request": "%s/fhir/$export",
              "requiresAccessToken": false,
              "output": [
                {"type": "Patient", "url": "%s/data/Patient.ndjson"}
              ],
              "error": []
            }
            """,
            baseUrl, baseUrl);

    wireMockServer.stubFor(
        get(urlEqualTo("/manifest.json"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(manifest)));

    // Stub Patient NDJSON file.
    final String patientNdjson =
        """
        {"resourceType":"Patient","id":"patient1"}
        """;
    wireMockServer.stubFor(
        get(urlEqualTo("/data/Patient.ndjson"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/fhir+ndjson")
                    .withBody(patientNdjson)));
  }

  private void setupManifestWithRequiresAccessToken() {
    final String baseUrl = "http://localhost:" + wireMockServer.port();

    // Stub manifest endpoint with requiresAccessToken: true.
    final String manifest =
        String.format(
            """
            {
              "transactionTime": "2025-11-28T00:00:00Z",
              "request": "%s/fhir/$export",
              "requiresAccessToken": true,
              "output": [
                {"type": "Patient", "url": "%s/data/Patient.ndjson"}
              ],
              "error": []
            }
            """,
            baseUrl, baseUrl);

    wireMockServer.stubFor(
        get(urlEqualTo("/manifest.json"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(manifest)));

    // Stub Patient NDJSON file.
    final String patientNdjson =
        """
        {"resourceType":"Patient","id":"patient1"}
        """;
    wireMockServer.stubFor(
        get(urlEqualTo("/data/Patient.ndjson"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/fhir+ndjson")
                    .withBody(patientNdjson)));
  }

  private void setupFailingManifestStub() {
    wireMockServer.stubFor(
        get(urlEqualTo("/manifest.json")).willReturn(aResponse().withStatus(500)));
  }

  private void setupEmptyManifestStub() {
    final String manifest =
        """
        {
          "transactionTime": "2025-11-28T00:00:00Z",
          "output": [],
          "error": []
        }
        """;

    wireMockServer.stubFor(
        get(urlEqualTo("/manifest.json"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(manifest)));
  }

  private void setupManifestWithMissingTypeStub() {
    final String baseUrl = "http://localhost:" + wireMockServer.port();
    final String manifest =
        String.format(
            """
            {
              "transactionTime": "2025-11-28T00:00:00Z",
              "output": [
                {"url": "%s/data/Patient.ndjson"}
              ],
              "error": []
            }
            """,
            baseUrl);

    wireMockServer.stubFor(
        get(urlEqualTo("/manifest.json"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(manifest)));
  }
}
