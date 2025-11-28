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
import java.util.Map;
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
    final Submission submission = createCompletedSubmission();
    final SubmissionResult result = new SubmissionResult(
        SUBMISSION_ID,
        ISO_FORMATTER.format(Instant.now()),
        List.of(
            new OutputFile("Patient", "https://fhir.example.org/output/Patient.ndjson", 100L),
            new OutputFile("Observation", "https://fhir.example.org/output/Observation.ndjson",
                500L)
        ),
        List.of(),
        false
    );

    final Binary binary = resultBuilder.buildStatusManifest(submission, result, SERVER_BASE_URL);

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
  void buildStatusManifestWithErrorFiles() {
    final Submission submission = createCompletedSubmission();
    final SubmissionResult result = new SubmissionResult(
        SUBMISSION_ID,
        ISO_FORMATTER.format(Instant.now()),
        List.of(),
        List.of(
            ErrorFile.create("https://fhir.example.org/errors/error.ndjson",
                Map.of("error", 5L, "warning", 10L))
        ),
        false
    );

    final Binary binary = resultBuilder.buildStatusManifest(submission, result, SERVER_BASE_URL);

    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains("\"error\"");
    assertThat(json).contains("error.ndjson");
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
  void buildStatusManifestWithAccessTokenRequired() {
    final Submission submission = createCompletedSubmission();
    final SubmissionResult result = new SubmissionResult(
        SUBMISSION_ID,
        ISO_FORMATTER.format(Instant.now()),
        List.of(),
        List.of(),
        true
    );

    final Binary binary = resultBuilder.buildStatusManifest(submission, result, SERVER_BASE_URL);

    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains("\"requiresAccessToken\" : true");
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
    return Submission.createPending(
            SUBMISSION_ID,
            new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
            Optional.empty()
        )
        .withManifestDetails(
            "https://example.org/manifest.json",
            "https://example.org/fhir",
            List.of(),
            null
        )
        .withState(SubmissionState.COMPLETED);
  }

  private Submission createProcessingSubmission() {
    return Submission.createPending(
            SUBMISSION_ID,
            new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
            Optional.empty()
        )
        .withManifestDetails(
            "https://example.org/manifest.json",
            "https://example.org/fhir",
            List.of(),
            null
        )
        .withState(SubmissionState.PROCESSING);
  }

}
