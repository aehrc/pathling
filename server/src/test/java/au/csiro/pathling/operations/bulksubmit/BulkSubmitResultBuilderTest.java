/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.Binary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for BulkSubmitResultBuilder.
 *
 * @author John Grimes
 */
class BulkSubmitResultBuilderTest {

  private static final String SERVER_BASE_URL = "https://fhir.example.org/fhir";
  private static final String SUBMISSION_ID = "test-submission-123";
  private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;

  private BulkSubmitResultBuilder resultBuilder;

  @BeforeEach
  void setUp() {
    resultBuilder = new BulkSubmitResultBuilder();
  }

  @Test
  void buildStatusManifestWithOutputFiles() {
    // Create submission with manifest job containing output files.
    final Submission submission = createCompletedSubmissionWithOutputFiles();

    final Binary binary = resultBuilder.buildStatusManifest(submission, null, SERVER_BASE_URL);

    assertThat(binary.getContentType()).isEqualTo("application/json");
    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains("\"transactionTime\"");
    assertThat(json).contains("\"requiresAccessToken\" : false");
    assertThat(json).contains("\"output\"");
    assertThat(json).contains("Patient");
    assertThat(json).contains("Observation");
    assertThat(json).contains(SUBMISSION_ID);
  }

  @Test
  void buildStatusManifestWithoutResult() {
    final Submission submission = createCompletedSubmission();

    final Binary binary = resultBuilder.buildStatusManifest(submission, null, SERVER_BASE_URL);

    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains("\"output\" : [ ]");
    assertThat(json).contains("\"error\" : [ ]");
    assertThat(json).contains(SUBMISSION_ID);
  }

  @Test
  void buildStatusManifestAlwaysSetsRequiresAccessTokenFalse() {
    // With the new manifest job aggregation, requiresAccessToken is always false.
    final Submission submission = createCompletedSubmission();

    final Binary binary = resultBuilder.buildStatusManifest(submission, null, SERVER_BASE_URL);

    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains("\"requiresAccessToken\" : false");
  }

  @Test
  void buildProcessingResponse() {
    final Submission submission = createProcessingSubmission();

    final Binary binary = resultBuilder.buildProcessingResponse(submission, SERVER_BASE_URL);

    assertThat(binary.getContentType()).isEqualTo("application/json");
    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains(SUBMISSION_ID);
    assertThat(json).contains("\"status\" : \"processing\"");
    assertThat(json).contains("\"message\" : \"Submission is being processed\"");
  }

  @Test
  void buildProcessingResponseForPendingSubmission() {
    final Submission submission = Submission.createPending(
        SUBMISSION_ID,
        new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
        Optional.empty()
    );

    final Binary binary = resultBuilder.buildProcessingResponse(submission, SERVER_BASE_URL);

    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains("\"status\" : \"pending\"");
  }

  // ========================================
  // Helper Methods
  // ========================================

  private Submission createCompletedSubmission() {
    final ManifestJob completedJob = ManifestJob.createPending(
            "manifest-job-1",
            "https://example.org/manifest.json",
            "https://example.org/fhir"
        )
        .withState(ManifestJobState.COMPLETED);

    return Submission.createPending(
            SUBMISSION_ID,
            new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
            Optional.empty()
        )
        .withManifestJob(completedJob)
        .withState(SubmissionState.COMPLETED);
  }

  private Submission createCompletedSubmissionWithOutputFiles() {
    final ManifestJob completedJob = ManifestJob.createPending(
            "manifest-job-1",
            "https://example.org/manifest.json",
            "https://example.org/fhir"
        )
        .withState(ManifestJobState.COMPLETED)
        .withOutputFiles(List.of(
            new OutputFile("Patient", "https://fhir.example.org/output/Patient.ndjson", 100L),
            new OutputFile("Observation", "https://fhir.example.org/output/Observation.ndjson",
                500L)
        ));

    return Submission.createPending(
            SUBMISSION_ID,
            new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
            Optional.empty()
        )
        .withManifestJob(completedJob)
        .withState(SubmissionState.COMPLETED);
  }

  private Submission createProcessingSubmission() {
    final ManifestJob processingJob = ManifestJob.createPending(
            "manifest-job-1",
            "https://example.org/manifest.json",
            "https://example.org/fhir"
        )
        .withState(ManifestJobState.PROCESSING);

    return Submission.createPending(
            SUBMISSION_ID,
            new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
            Optional.empty()
        )
        .withManifestJob(processingJob)
        .withState(SubmissionState.PROCESSING);
  }

}
