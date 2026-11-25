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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for BulkSubmitResultBuilder.
 *
 * @author John Grimes
 */
class BulkSubmitResultBuilderTest {

  private static final String FHIR_SERVER_BASE = "https://fhir.example.org/fhir";
  private static final String REQUEST_URL =
      FHIR_SERVER_BASE
          + "/$bulk-submit-status?submissionId=test-submission-123"
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

    final Parameters parameters =
        resultBuilder.buildStatusManifest(submission, REQUEST_URL, FHIR_SERVER_BASE);

    // Verify request parameter exists with correct value.
    final ParametersParameterComponent requestParam = findParameter(parameters, "request");
    assertThat(requestParam.getValue().primitiveValue()).isEqualTo(REQUEST_URL);
  }

  @Test
  void buildStatusManifestHasEmptyOutputArrayWhenNoDownloads() {
    // When there are no downloaded files, there are no output parameters.
    final Submission submission = createCompletedSubmission();

    final Parameters parameters =
        resultBuilder.buildStatusManifest(submission, REQUEST_URL, FHIR_SERVER_BASE);

    // Verify no output parameters.
    final long outputCount =
        parameters.getParameter().stream().filter(p -> "output".equals(p.getName())).count();
    assertThat(outputCount).isZero();

    // Verify no error parameters.
    final long errorCount =
        parameters.getParameter().stream().filter(p -> "error".equals(p.getName())).count();
    assertThat(errorCount).isZero();

    // Verify extension contains submission ID.
    final ParametersParameterComponent extParam = findParameter(parameters, "extension");
    final String valueString =
        extParam.getPart().stream()
            .filter(p -> "valueString".equals(p.getName()))
            .map(p -> p.getValue().primitiveValue())
            .findFirst()
            .orElse(null);
    assertThat(valueString).isEqualTo(SUBMISSION_ID);
  }

  @Test
  void buildStatusManifestIncludesDownloadedFilesInOutput() {
    // When files have been downloaded, the output parameters contain result URLs.
    final Submission submission = createSubmissionWithDownloadedFiles();

    final Parameters parameters =
        resultBuilder.buildStatusManifest(submission, REQUEST_URL, FHIR_SERVER_BASE);

    // Verify output parameters exist.
    final long outputCount =
        parameters.getParameter().stream().filter(p -> "output".equals(p.getName())).count();
    assertThat(outputCount).isEqualTo(2);

    // Verify Patient output.
    final ParametersParameterComponent patientOutput =
        parameters.getParameter().stream()
            .filter(p -> "output".equals(p.getName()))
            .filter(
                p ->
                    p.getPart().stream()
                        .anyMatch(
                            part ->
                                "type".equals(part.getName())
                                    && "Patient".equals(part.getValue().primitiveValue())))
            .findFirst()
            .orElseThrow();

    final String patientUrl =
        patientOutput.getPart().stream()
            .filter(p -> "url".equals(p.getName()))
            .map(p -> p.getValue().primitiveValue())
            .findFirst()
            .orElse(null);
    // The result URL refers to the server-generated staging ID, not the client-chosen submission
    // ID, as the staging ID is the key under which the files are stored and their owner recorded.
    assertThat(patientUrl)
        .contains(
            "$result?job=" + submission.stagingId() + "&file=Patient.manifest-job-1-1.ndjson");
  }

  @Test
  void buildStatusManifestSetsRequiresAccessTokenFalse() {
    final Submission submission = createCompletedSubmission();

    final Parameters parameters =
        resultBuilder.buildStatusManifest(submission, REQUEST_URL, FHIR_SERVER_BASE);

    final ParametersParameterComponent tokenParam =
        findParameter(parameters, "requiresAccessToken");
    assertThat(tokenParam.getValue().primitiveValue()).isEqualTo("false");
  }

  @Test
  void buildStatusManifestIncludesSubmissionIdExtension() {
    // The manifest should include the submission ID in an extension.
    final Submission submission = createCompletedSubmission();

    final Parameters parameters =
        resultBuilder.buildStatusManifest(submission, REQUEST_URL, FHIR_SERVER_BASE);

    final ParametersParameterComponent extParam = findParameter(parameters, "extension");

    // Verify extension URL.
    final String url =
        extParam.getPart().stream()
            .filter(p -> "url".equals(p.getName()))
            .map(p -> p.getValue().primitiveValue())
            .findFirst()
            .orElse(null);
    assertThat(url)
        .isEqualTo("http://hl7.org/fhir/uv/bulkdata/StructureDefinition/bulk-submit-submission-id");

    // Verify extension value.
    final String value =
        extParam.getPart().stream()
            .filter(p -> "valueString".equals(p.getName()))
            .map(p -> p.getValue().primitiveValue())
            .findFirst()
            .orElse(null);
    assertThat(value).isEqualTo(SUBMISSION_ID);
  }

  @Test
  void buildStatusManifestIncludesTransactionTimeWhenCompleted() {
    // For completed submissions, the transactionTime should be included.
    final Submission submission = createCompletedSubmission();

    final Parameters parameters =
        resultBuilder.buildStatusManifest(submission, REQUEST_URL, FHIR_SERVER_BASE);

    final ParametersParameterComponent timeParam = findParameter(parameters, "transactionTime");
    assertThat(timeParam.getValue()).isNotNull();
  }

  @Test
  void buildStatusManifestForProcessingSubmission() {
    // The same manifest structure is used for in-progress submissions (HTTP 202).
    final Submission submission = createProcessingSubmission();

    final Parameters parameters =
        resultBuilder.buildStatusManifest(submission, REQUEST_URL, FHIR_SERVER_BASE);

    // Verify required parameters exist.
    assertThat(findParameter(parameters, "request")).isNotNull();
    assertThat(findParameter(parameters, "requiresAccessToken")).isNotNull();
    assertThat(findParameter(parameters, "extension")).isNotNull();
  }

  @Test
  void buildStatusManifestForPendingSubmission() {
    // Pending submissions use the same manifest structure.
    final Submission submission =
        Submission.createPending(
            SUBMISSION_ID,
            new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
            Optional.empty());

    final Parameters parameters =
        resultBuilder.buildStatusManifest(submission, REQUEST_URL, FHIR_SERVER_BASE);

    // Verify required parameters exist.
    assertThat(findParameter(parameters, "request")).isNotNull();
    assertThat(findParameter(parameters, "extension")).isNotNull();

    // Pending submissions don't have transactionTime yet.
    final boolean hasTransactionTime =
        parameters.getParameter().stream().anyMatch(p -> "transactionTime".equals(p.getName()));
    assertThat(hasTransactionTime).isFalse();
  }

  // ========================================
  // Type mismatch error tests
  // ========================================

  @Test
  void buildStatusManifestReportsTypeMismatchFileOfSuccessfulManifestJob() {
    // A manifest job that completed but skipped resources of an unexpected type reports the file
    // describing them in the error section, linked to the manifest it came from.
    final String manifestUrl = "https://example.org/manifest.json";
    final ManifestJob job =
        ManifestJob.createPending("manifest-job-1", manifestUrl, "https://example.org/fhir", null)
            .withTypeMismatchFileName("OperationOutcome.manifest-job-1-type-mismatch.ndjson")
            .withState(ManifestJobState.COMPLETED);
    final Submission submission = createSubmissionWithManifestJob(job);

    final Parameters parameters =
        resultBuilder.buildStatusManifest(submission, REQUEST_URL, FHIR_SERVER_BASE);

    final List<ParametersParameterComponent> errors = findParameters(parameters, "error");
    assertThat(errors).hasSize(1);
    final ParametersParameterComponent error = errors.getFirst();
    assertThat(partValue(error, "type")).isEqualTo("OperationOutcome");
    assertThat(partValue(error, "url"))
        .isEqualTo(
            FHIR_SERVER_BASE
                + "/$result?job="
                + submission.stagingId()
                + "&file=OperationOutcome.manifest-job-1-type-mismatch.ndjson");
    assertThat(manifestUrlExtensionValue(error)).isEqualTo(manifestUrl);
  }

  @Test
  void buildStatusManifestReportsTypeMismatchFileOfDownloadedManifestJob() {
    // The error is reported as soon as the download finishes, before the submission is complete.
    final ManifestJob job =
        ManifestJob.createPending("manifest-job-1", "https://example.org/manifest.json", null, null)
            .withTypeMismatchFileName("OperationOutcome.manifest-job-1-type-mismatch.ndjson")
            .withState(ManifestJobState.DOWNLOADED);
    final Submission submission = createSubmissionWithManifestJob(job);

    final Parameters parameters =
        resultBuilder.buildStatusManifest(submission, REQUEST_URL, FHIR_SERVER_BASE);

    assertThat(findParameters(parameters, "error")).hasSize(1);
  }

  @Test
  void buildStatusManifestReportsBothTypeMismatchAndFailureFiles() {
    // When the import later fails, the type mismatch file is still reported alongside the file
    // describing the failure.
    final String manifestUrl = "https://example.org/manifest.json";
    final ManifestJob job =
        ManifestJob.createPending("manifest-job-1", manifestUrl, null, null)
            .withTypeMismatchFileName("OperationOutcome.manifest-job-1-type-mismatch.ndjson")
            .withState(ManifestJobState.DOWNLOADED)
            .withError("Import failed", "OperationOutcome.manifest-job-1-error.ndjson");
    final Submission submission = createSubmissionWithManifestJob(job);

    final Parameters parameters =
        resultBuilder.buildStatusManifest(submission, REQUEST_URL, FHIR_SERVER_BASE);

    final List<ParametersParameterComponent> errors = findParameters(parameters, "error");
    assertThat(errors)
        .extracting(error -> partValue(error, "url"))
        .containsExactlyInAnyOrder(
            FHIR_SERVER_BASE
                + "/$result?job="
                + submission.stagingId()
                + "&file=OperationOutcome.manifest-job-1-type-mismatch.ndjson",
            FHIR_SERVER_BASE
                + "/$result?job="
                + submission.stagingId()
                + "&file=OperationOutcome.manifest-job-1-error.ndjson");
    assertThat(errors).extracting(this::manifestUrlExtensionValue).containsOnly(manifestUrl);
  }

  // ========================================
  // Helper Methods
  // ========================================

  private List<ParametersParameterComponent> findParameters(
      final Parameters parameters, final String name) {
    return parameters.getParameter().stream().filter(p -> name.equals(p.getName())).toList();
  }

  private String partValue(final ParametersParameterComponent parameter, final String name) {
    return parameter.getPart().stream()
        .filter(p -> name.equals(p.getName()))
        .map(p -> p.getValue().primitiveValue())
        .findFirst()
        .orElseThrow(() -> new AssertionError("Part not found: " + name));
  }

  private String manifestUrlExtensionValue(final ParametersParameterComponent parameter) {
    return parameter.getPart().stream()
        .filter(p -> "extension".equals(p.getName()))
        .filter(p -> "manifestUrl".equals(partValue(p, "url")))
        .map(p -> partValue(p, "valueUrl"))
        .findFirst()
        .orElseThrow(() -> new AssertionError("manifestUrl extension not found"));
  }

  private Submission createSubmissionWithManifestJob(final ManifestJob job) {
    return Submission.createPending(
            SUBMISSION_ID,
            new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
            Optional.empty())
        .withManifestJob(job)
        .withState(SubmissionState.PROCESSING);
  }

  private ParametersParameterComponent findParameter(
      final Parameters parameters, final String name) {
    return parameters.getParameter().stream()
        .filter(p -> name.equals(p.getName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Parameter not found: " + name));
  }

  private Submission createCompletedSubmission() {
    final ManifestJob completedJob =
        ManifestJob.createPending(
                "manifest-job-1",
                "https://example.org/manifest.json",
                "https://example.org/fhir",
                null)
            .withState(ManifestJobState.COMPLETED);

    return Submission.createPending(
            SUBMISSION_ID,
            new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
            Optional.empty())
        .withManifestJob(completedJob)
        .withState(SubmissionState.COMPLETED);
  }

  private Submission createProcessingSubmission() {
    final ManifestJob processingJob =
        ManifestJob.createPending(
                "manifest-job-1",
                "https://example.org/manifest.json",
                "https://example.org/fhir",
                null)
            .withState(ManifestJobState.PROCESSING);

    return Submission.createPending(
            SUBMISSION_ID,
            new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
            Optional.empty())
        .withManifestJob(processingJob)
        .withState(SubmissionState.PROCESSING);
  }

  private Submission createSubmissionWithDownloadedFiles() {
    final String manifestUrl = "https://example.org/manifest.json";
    final ManifestJob downloadedJob =
        ManifestJob.createPending("manifest-job-1", manifestUrl, "https://example.org/fhir", null)
            .withDownloadedFiles(
                java.util.List.of(
                    new DownloadedFile(
                        "Patient",
                        "Patient.manifest-job-1-1.ndjson",
                        "file:///tmp/Patient.manifest-job-1-1.ndjson",
                        manifestUrl),
                    new DownloadedFile(
                        "Observation",
                        "Observation.manifest-job-1-2.ndjson",
                        "file:///tmp/Observation.manifest-job-1-2.ndjson",
                        manifestUrl)))
            .withState(ManifestJobState.DOWNLOADED);

    return Submission.createPending(
            SUBMISSION_ID,
            new SubmitterIdentifier("https://example.org/submitters", "test-submitter"),
            Optional.empty())
        .withManifestJob(downloadedJob)
        .withState(SubmissionState.PROCESSING);
  }
}
