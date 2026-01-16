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

package au.csiro.pathling.operations.bulkimport;

import au.csiro.pathling.ParamUtil;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.operations.OperationValidation;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UrlType;
import org.springframework.stereotype.Component;

/**
 * Validates ping and pull import operation requests in FHIR Parameters format.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class ImportPnpOperationValidator {

  private static final String EXPORT_TYPE_DYNAMIC = "dynamic";
  private static final String EXPORT_TYPE_STATIC = "static";

  /**
   * Validates a ping and pull import request from a FHIR Parameters resource.
   *
   * @param requestDetails the request details
   * @param parameters the FHIR Parameters resource
   * @return the validation result containing the ImportPnpRequest and any issues
   */
  public PreAsyncValidationResult<ImportPnpRequest> validateParametersRequest(
      @Nonnull final RequestDetails requestDetails, @Nonnull final Parameters parameters) {
    // Extract exportUrl parameter (required).
    final String exportUrl =
        ParamUtil.extractFromPart(
                parameters.getParameter(),
                "exportUrl",
                UrlType.class,
                UrlType::getValue,
                false,
                Optional.empty(),
                false,
                Optional.of(new InvalidUserInputError("Missing required parameter: exportUrl")))
            .orElseThrow(() -> new InvalidUserInputError("exportUrl must not be null"));

    // Extract exportType parameter (optional, defaults to "dynamic").
    final String exportType =
        ParamUtil.extractFromPart(
                parameters.getParameter(),
                "exportType",
                CodeType.class,
                CodeType::getCode,
                true,
                Optional.of(EXPORT_TYPE_DYNAMIC),
                false,
                Optional.of(new InvalidUserInputError("Invalid exportType")))
            .orElseThrow();

    // Validate exportType.
    if (!EXPORT_TYPE_DYNAMIC.equals(exportType) && !EXPORT_TYPE_STATIC.equals(exportType)) {
      throw new InvalidUserInputError(
          "Invalid exportType: %s. Must be 'dynamic' or 'static'.".formatted(exportType));
    }

    // Note: inputSource parameter is accepted but ignored for backwards compatibility.

    // Extract saveMode parameter (optional, defaults to OVERWRITE).
    final SaveMode saveMode =
        ParamUtil.extractFromPart(
                parameters.getParameter(),
                "saveMode",
                CodeType.class,
                code -> SaveMode.fromCode(code.getCode()),
                true,
                Optional.of(SaveMode.OVERWRITE),
                false,
                Optional.of(new InvalidUserInputError("Unknown saveMode.")))
            .orElseThrow();

    // Extract inputFormat parameter (optional, defaults to NDJSON).
    final ImportFormat importFormat =
        ParamUtil.extractFromPart(
                parameters.getParameter(),
                "inputFormat",
                CodeType.class,
                code -> parseImportFormat(code.getCode()),
                true,
                Optional.of(ImportFormat.NDJSON),
                false,
                Optional.of(new InvalidUserInputError("Unknown format.")))
            .orElseThrow();

    // Extract Bulk Data Export passthrough parameters.
    final List<String> types = extractStringList(parameters.getParameter(), "_type");
    final Optional<Instant> since = extractInstant(parameters.getParameter(), "_since");
    final Optional<Instant> until = extractInstant(parameters.getParameter(), "_until");
    final List<String> elements = extractStringList(parameters.getParameter(), "_elements");
    final List<String> typeFilters = extractStringList(parameters.getParameter(), "_typeFilter");
    final List<String> includeAssociatedData =
        extractCodeList(parameters.getParameter(), "includeAssociatedData");

    final ImportPnpRequest importPnpRequest =
        new ImportPnpRequest(
            requestDetails.getCompleteUrl(),
            exportUrl,
            exportType,
            saveMode,
            importFormat,
            types,
            since,
            until,
            elements,
            typeFilters,
            includeAssociatedData);

    final List<OperationOutcome.OperationOutcomeIssueComponent> issues =
        Stream.of(
                OperationValidation.validateAcceptHeader(requestDetails, false),
                OperationValidation.validatePreferHeader(requestDetails, false))
            .flatMap(Collection::stream)
            .toList();

    return new PreAsyncValidationResult<>(importPnpRequest, issues);
  }

  /**
   * Parses an import format string from MIME type (e.g., "application/fhir+ndjson").
   *
   * @param formatString the format string
   * @return the ImportFormat
   */
  private ImportFormat parseImportFormat(final String formatString) {
    if (formatString == null || formatString.isBlank()) {
      return ImportFormat.NDJSON; // Default.
    }
    try {
      return ImportFormat.fromCode(formatString);
    } catch (final IllegalArgumentException e) {
      throw new InvalidUserInputError(e.getMessage());
    }
  }

  /**
   * Extracts a list of string values from multiple parameters with the same name.
   *
   * @param parts the parameter parts to search
   * @param paramName the name of the parameter
   * @return the list of string values, or empty list if not found
   */
  @Nonnull
  private List<String> extractStringList(
      @Nonnull final List<ParametersParameterComponent> parts, @Nonnull final String paramName) {
    return parts.stream()
        .filter(param -> paramName.equals(param.getName()))
        .filter(param -> param.getValue() instanceof StringType)
        .map(param -> ((StringType) param.getValue()).getValue())
        .toList();
  }

  /**
   * Extracts a list of code values from multiple parameters with the same name.
   *
   * @param parts the parameter parts to search
   * @param paramName the name of the parameter
   * @return the list of code values, or empty list if not found
   */
  @Nonnull
  private List<String> extractCodeList(
      @Nonnull final List<ParametersParameterComponent> parts, @Nonnull final String paramName) {
    return parts.stream()
        .filter(param -> paramName.equals(param.getName()))
        .filter(param -> param.getValue() instanceof CodeType)
        .map(param -> ((CodeType) param.getValue()).getCode())
        .toList();
  }

  /**
   * Extracts an optional instant value from a parameter.
   *
   * @param parts the parameter parts to search
   * @param paramName the name of the parameter
   * @return the instant value wrapped in Optional, or empty if not found
   */
  @Nonnull
  private Optional<Instant> extractInstant(
      @Nonnull final List<ParametersParameterComponent> parts, @Nonnull final String paramName) {
    return parts.stream()
        .filter(param -> paramName.equals(param.getName()))
        .filter(param -> param.getValue() instanceof InstantType)
        .map(param -> ((InstantType) param.getValue()).getValue().toInstant())
        .findFirst();
  }
}
