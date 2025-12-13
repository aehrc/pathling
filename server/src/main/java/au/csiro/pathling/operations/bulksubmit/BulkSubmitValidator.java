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

import au.csiro.pathling.ParamUtil;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.config.BulkSubmitConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.operations.OperationValidation;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.stereotype.Component;

/**
 * Validates $bulk-submit operation requests.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Slf4j
@Component
public class BulkSubmitValidator {

  private static final Set<String> VALID_STATUSES = Set.of(
      BulkSubmitRequest.STATUS_IN_PROGRESS,
      BulkSubmitRequest.STATUS_COMPLETE,
      BulkSubmitRequest.STATUS_ABORTED
  );

  @Nonnull
  private final ServerConfiguration serverConfiguration;

  /**
   * Creates a new BulkSubmitValidator.
   *
   * @param serverConfiguration The server configuration.
   */
  public BulkSubmitValidator(@Nonnull final ServerConfiguration serverConfiguration) {
    this.serverConfiguration = serverConfiguration;
  }

  /**
   * Validates a $bulk-submit request from a FHIR Parameters resource.
   *
   * @param requestDetails The request details.
   * @param parameters The FHIR Parameters resource.
   * @return The validation result containing the BulkSubmitRequest and any issues.
   */
  @Nonnull
  public PreAsyncValidationResult<BulkSubmitRequest> validateRequest(
      @Nonnull final RequestDetails requestDetails,
      @Nonnull final Parameters parameters
  ) {
    final BulkSubmitConfiguration config = getBulkSubmitConfig();

    // Extract submissionId (required).
    final String submissionId = extractRequiredString(parameters, "submissionId");

    // Extract submitter (required).
    final SubmitterIdentifier submitter = extractSubmitter(parameters);

    // TODO: Re-enable allowed submitters check after connectathon.
    // Validate submitter is allowed.
    // if (!config.isSubmitterAllowed(submitter)) {
    //   throw new InvalidUserInputError(
    //       "Submitter %s|%s is not in the list of allowed submitters."
    //           .formatted(submitter.system(), submitter.value())
    //   );
    // }

    // Extract submissionStatus (required).
    final String submissionStatus = extractSubmissionStatus(parameters);

    // Modify the complete URL to include submissionId and status as query parameters. This ensures
    // different $bulk-submit requests (e.g., in-progress vs complete) get different async job
    // cache tags, so each is processed independently rather than returning a cached response.
    final String originalUrl = requestDetails.getCompleteUrl();
    final String separator = originalUrl.contains("?")
                             ? "&"
                             : "?";
    requestDetails.setCompleteUrl(originalUrl + separator + "_submissionId=" + submissionId
        + "&_submissionStatus=" + submissionStatus);

    // Extract manifestUrl (conditionally required).
    final String manifestUrl = extractOptionalUrl(parameters, "manifestUrl");

    // Extract fhirBaseUrl (conditionally required). Accept both fhirBaseUrl and FHIRBaseUrl per spec
    // inconsistency.
    String fhirBaseUrl = extractOptionalUrl(parameters, "fhirBaseUrl");
    if (fhirBaseUrl == null) {
      fhirBaseUrl = extractOptionalUrl(parameters, "FHIRBaseUrl");
    }

    // Validate conditional requirements.
    // Per Argonaut spec, manifestUrl may be omitted when setting submissionStatus to complete or
    // aborted. The manifest details can come from a previous in-progress request. However, if
    // manifestUrl is provided, fhirBaseUrl must also be provided.
    if (manifestUrl != null) {
      if (fhirBaseUrl == null) {
        throw new InvalidUserInputError(
            "fhirBaseUrl is required when manifestUrl is present.");
      }
      validateUrl(manifestUrl, "manifestUrl", config);
      validateUrl(fhirBaseUrl, "fhirBaseUrl", config);
    }

    // Extract replacesManifestUrl (optional).
    final String replacesManifestUrl = extractOptionalUrl(parameters, "replacesManifestUrl");
    if (replacesManifestUrl != null) {
      validateUrl(replacesManifestUrl, "replacesManifestUrl", config);
    }

    // Extract metadata (optional).
    final SubmissionMetadata metadata = extractMetadata(parameters);

    final BulkSubmitRequest request = new BulkSubmitRequest(
        requestDetails.getCompleteUrl(),
        submissionId,
        submitter,
        submissionStatus,
        manifestUrl,
        fhirBaseUrl,
        replacesManifestUrl,
        metadata
    );

    final List<OperationOutcome.OperationOutcomeIssueComponent> issues = Stream.of(
            OperationValidation.validateAcceptHeader(requestDetails, true),
            OperationValidation.validatePreferHeader(requestDetails, true))
        .flatMap(Collection::stream)
        .toList();

    return new PreAsyncValidationResult<>(request, issues);
  }

  @Nonnull
  private BulkSubmitConfiguration getBulkSubmitConfig() {
    final BulkSubmitConfiguration config = serverConfiguration.getBulkSubmit();
    if (config == null) {
      throw new InvalidUserInputError("The $bulk-submit operation is not configured.");
    }
    return config;
  }

  @Nonnull
  private String extractRequiredString(
      @Nonnull final Parameters parameters,
      @Nonnull final String paramName
  ) {
    final String value = ParamUtil.extractFromPart(
        parameters.getParameter(),
        paramName,
        StringType.class,
        StringType::getValue,
        false,
        null,
        false,
        new InvalidUserInputError("Missing required parameter: " + paramName)
    );
    if (value == null || value.isBlank()) {
      throw new InvalidUserInputError("Missing required parameter: " + paramName);
    }
    return value;
  }

  @Nonnull
  private SubmitterIdentifier extractSubmitter(@Nonnull final Parameters parameters) {
    final Identifier identifier = ParamUtil.extractFromPart(
        parameters.getParameter(),
        "submitter",
        Identifier.class,
        i -> i,
        false,
        null,
        false,
        new InvalidUserInputError("Missing required parameter: submitter")
    );
    if (identifier == null) {
      throw new InvalidUserInputError("Missing required parameter: submitter");
    }
    if (identifier.getSystem() == null || identifier.getSystem().isBlank()) {
      throw new InvalidUserInputError("submitter.system is required.");
    }
    if (identifier.getValue() == null || identifier.getValue().isBlank()) {
      throw new InvalidUserInputError("submitter.value is required.");
    }
    return new SubmitterIdentifier(identifier.getSystem(), identifier.getValue());
  }

  @Nonnull
  private String extractSubmissionStatus(@Nonnull final Parameters parameters) {
    final String status = ParamUtil.extractFromPart(
        parameters.getParameter(),
        "submissionStatus",
        Coding.class,
        Coding::getCode,
        false,
        null,
        false,
        new InvalidUserInputError("Missing required parameter: submissionStatus")
    );
    if (status == null || status.isBlank()) {
      throw new InvalidUserInputError("Missing required parameter: submissionStatus");
    }
    if (!VALID_STATUSES.contains(status)) {
      throw new InvalidUserInputError(
          "Invalid submissionStatus: %s. Must be one of: %s.".formatted(status, VALID_STATUSES)
      );
    }
    return status;
  }

  @Nullable
  private String extractOptionalUrl(
      @Nonnull final Parameters parameters,
      @Nonnull final String paramName
  ) {
    // Per Argonaut spec, URL parameters are string (url), not FHIR url type.
    return ParamUtil.extractFromPart(
        parameters.getParameter(),
        paramName,
        StringType.class,
        StringType::getValue,
        true,
        null,
        false,
        null
    );
  }

  private void validateUrl(
      @Nonnull final String url,
      @Nonnull final String paramName,
      @Nonnull final BulkSubmitConfiguration config
  ) {
    // TODO: Re-enable allowed sources check after connectathon.
    if (true) {
      return;
    }
    final List<String> allowableSources = config.getAllowableSources();
    if (allowableSources.isEmpty()) {
      return;
    }
    final boolean allowed = allowableSources.stream().anyMatch(url::startsWith);
    if (!allowed) {
      throw new InvalidUserInputError(
          "%s '%s' does not match any allowed source prefixes.".formatted(paramName, url)
      );
    }
  }

  @Nullable
  private SubmissionMetadata extractMetadata(@Nonnull final Parameters parameters) {
    // Metadata extraction is simplified for now - can be expanded later.
    return null;
  }

}
