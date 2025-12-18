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

  private static final String REQUEST_URL =
      "https://fhir.example.org/fhir/$bulk-submit-status?submissionId=test-submission-123"
          + "&submitter=https://example.org/submitters|test-submitter";
  private static final String SUBMISSION_ID = "test-submission-123";

  private BulkSubmitResultBuilder resultBuilder;

  @BeforeEach
  void setUp() {
    resultBuilder = new BulkSubmitResultBuilder();
  }

  @Test
  void buildStatusManifestIncludesRequestUrl() {
    // The manifest should include the request URL.
    final Submission submission = createCompletedSubmission();

    final Binary binary = resultBuilder.buildStatusManifest(submission, REQUEST_URL);

    assertThat(binary.getContentType()).isEqualTo("application/json");
    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains("\"request\" : \"" + REQUEST_URL + "\"");
  }

  @Test
  void buildStatusManifestHasEmptyOutputArray() {
    // Bulk submit imports data, so there are no output files to download.
    final Submission submission = createCompletedSubmission();

    final Binary binary = resultBuilder.buildStatusManifest(submission, REQUEST_URL);

    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains("\"output\" : [ ]");
    assertThat(json).contains("\"error\" : [ ]");
    assertThat(json).contains(SUBMISSION_ID);
  }

  @Test
  void buildStatusManifestSetsRequiresAccessTokenFalse() {
    final Submission submission = createCompletedSubmission();

    final Binary binary = resultBuilder.buildStatusManifest(submission, REQUEST_URL);

    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains("\"requiresAccessToken\" : false");
  }

  @Test
  void buildStatusManifestIncludesSubmissionIdExtension() {
    // The manifest should include the submission ID in an extension.
    final Submission submission = createCompletedSubmission();

    final Binary binary = resultBuilder.buildStatusManifest(submission, REQUEST_URL);

    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains(
        "\"url\" : \"http://hl7.org/fhir/uv/bulkdata/StructureDefinition/bulk-submit-submission-id\"");
    assertThat(json).contains("\"valueString\" : \"" + SUBMISSION_ID + "\"");
  }

  @Test
  void buildStatusManifestIncludesTransactionTimeWhenCompleted() {
    // For completed submissions, the transactionTime should be included.
    final Submission submission = createCompletedSubmission();

    final Binary binary = resultBuilder.buildStatusManifest(submission, REQUEST_URL);

    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains("\"transactionTime\"");
  }

  @Test
  void buildStatusManifestForProcessingSubmission() {
    // The same manifest structure is used for in-progress submissions (HTTP 202).
    final Submission submission = createProcessingSubmission();

    final Binary binary = resultBuilder.buildStatusManifest(submission, REQUEST_URL);

    assertThat(binary.getContentType()).isEqualTo("application/json");
    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains(SUBMISSION_ID);
    assertThat(json).contains("\"request\" : \"" + REQUEST_URL + "\"");
    assertThat(json).contains("\"requiresAccessToken\" : false");
    assertThat(json).contains("\"output\" : [ ]");
    assertThat(json).contains("\"error\" : [ ]");
  }

  @Test
  void buildStatusManifestForPendingSubmission() {
    // Pending submissions use the same manifest structure.
    final Submission submission = Submission.createPending(
        SUBMISSION_ID,
        new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
        Optional.empty()
    );

    final Binary binary = resultBuilder.buildStatusManifest(submission, REQUEST_URL);

    final String json = new String(binary.getData(), StandardCharsets.UTF_8);
    assertThat(json).contains(SUBMISSION_ID);
    assertThat(json).contains("\"request\" : \"" + REQUEST_URL + "\"");
    // Pending submissions don't have transactionTime yet.
    assertThat(json).doesNotContain("\"transactionTime\"");
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
