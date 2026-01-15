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

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.ParamUtil;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.operations.OperationValidation;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;
import org.springframework.stereotype.Component;

/**
 * Validates import operation requests in both JSON (SMART Bulk Data Import) and FHIR Parameters
 * formats.
 *
 * @author Felix Naumann
 */
@Slf4j
@Component
public class ImportOperationValidator {

  /**
   * Validates an import request from a FHIR Parameters resource.
   *
   * @param requestDetails the request details
   * @param parameters the FHIR Parameters resource
   * @return the validation result containing the ImportRequest and any issues
   */
  public PreAsyncValidation.PreAsyncValidationResult<ImportRequest> validateParametersRequest(
      @Nonnull final RequestDetails requestDetails, @Nonnull final Parameters parameters) {
    // Note: inputSource parameter is accepted but ignored for backwards compatibility.

    // Extract input parameters.
    final Collection<ParametersParameterComponent> inputParts =
        ParamUtil.extractManyFromParameters(
                parameters.getParameter(),
                "input",
                ParametersParameterComponent.class,
                false,
                Optional.of(Collections.emptyList()),
                false,
                Optional.of(new InvalidUserInputError("The input may not be empty.")))
            .orElseThrow();

    final Map<String, Collection<String>> input =
        inputParts.stream()
            .map(this::mapInputFieldsFromParameters)
            .filter(Objects::nonNull)
            .peek(this::validateResourceType)
            .collect(
                Collectors.groupingBy(
                    InputParams::resourceType,
                    Collectors.mapping(InputParams::url, Collectors.toCollection(ArrayList::new))));

    final SaveMode saveMode = getSaveModeFromParameters(parameters);
    final ImportFormat importFormat = getImportFormatFromParameters(parameters);
    final ImportRequest importRequest =
        new ImportRequest(requestDetails.getCompleteUrl(), input, saveMode, importFormat);

    final List<OperationOutcome.OperationOutcomeIssueComponent> issues =
        Stream.of(
                OperationValidation.validateAcceptHeader(requestDetails, false),
                OperationValidation.validatePreferHeader(requestDetails, false))
            .flatMap(Collection::stream)
            .toList();

    return new PreAsyncValidationResult<>(importRequest, issues);
  }

  /**
   * Validates an import request from a JSON manifest (SMART Bulk Data Import format).
   *
   * @param requestDetails the request details
   * @param manifest the JSON manifest
   * @return the validation result containing the ImportRequest and any issues
   */
  @SuppressWarnings({"ConstantValue", "ConstantConditions", "java:S2583"})
  // Null checks needed despite @Nonnull - Jackson can deserialize nulls.
  public PreAsyncValidation.PreAsyncValidationResult<ImportRequest> validateJsonRequest(
      @Nonnull final RequestDetails requestDetails, @Nonnull final ImportManifest manifest) {
    // Validate required fields (null checks needed because Jackson may deserialize nulls).
    // Note: inputSource is accepted but ignored for backwards compatibility.
    if (manifest.inputFormat() == null || manifest.inputFormat().isBlank()) {
      throw new InvalidUserInputError("Missing required field: inputFormat");
    }
    if (manifest.input() == null || manifest.input().isEmpty()) {
      throw new InvalidUserInputError("The input may not be empty.");
    }

    // Parse and validate input array.
    final Map<String, Collection<String>> input =
        manifest.input().stream()
            .map(this::mapInputFieldsFromJson)
            .peek(this::validateResourceType)
            .collect(
                Collectors.groupingBy(
                    InputParams::resourceType,
                    Collectors.mapping(InputParams::url, Collectors.toCollection(ArrayList::new))));

    // Parse format and mode.
    final ImportFormat importFormat = parseImportFormat(manifest.inputFormat());
    final SaveMode saveMode =
        manifest.mode() != null && !manifest.mode().isBlank()
            ? SaveMode.fromCode(manifest.mode())
            : SaveMode.OVERWRITE; // Default to OVERWRITE.

    final ImportRequest importRequest =
        new ImportRequest(requestDetails.getCompleteUrl(), input, saveMode, importFormat);

    final List<OperationOutcome.OperationOutcomeIssueComponent> issues =
        Stream.of(
                OperationValidation.validateAcceptHeader(requestDetails, false),
                OperationValidation.validatePreferHeader(requestDetails, false))
            .flatMap(Collection::stream)
            .toList();

    return new PreAsyncValidationResult<>(importRequest, issues);
  }

  private void validateResourceType(final InputParams inputParams) {
    final String resourceType = inputParams.resourceType();
    try {
      final boolean supported =
          FhirServer.supportedResourceTypes().contains(ResourceType.fromCode(resourceType));
      if (!supported) {
        throw new InvalidUserInputError(
            "The resource type '%s' is not supported.".formatted(resourceType));
      }
    } catch (final FHIRException e) {
      // Check if this is a custom resource type (e.g., ViewDefinition).
      if (!FhirServer.isCustomResourceType(resourceType)) {
        throw new InvalidUserInputError("Unknown resource type.", e);
      }
    }
  }

  private ImportFormat getImportFormatFromParameters(final Parameters parameters) {
    return ParamUtil.extractFromPart(
            parameters.getParameter(),
            "inputFormat",
            CodeType.class,
            code -> parseImportFormat(code.getCode()),
            true,
            Optional.of(ImportFormat.NDJSON), // Default to NDJSON.
            false,
            Optional.of(new InvalidUserInputError("Unknown format.")))
        .orElseThrow();
  }

  private SaveMode getSaveModeFromParameters(final Parameters parameters) {
    return ParamUtil.extractFromPart(
            parameters.getParameter(),
            "saveMode",
            CodeType.class,
            code -> SaveMode.fromCode(code.getCode()),
            true,
            Optional.of(SaveMode.OVERWRITE), // Default to OVERWRITE.
            false,
            Optional.of(new InvalidUserInputError("Unknown saveMode.")))
        .orElseThrow();
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

  private record InputParams(String resourceType, String url) {}

  @Nullable
  private InputParams mapInputFieldsFromParameters(final ParametersParameterComponent part) {
    final List<ParametersParameterComponent> partContainingResourceTypeAndUrl = part.getPart();
    final String resourceType = getResourceTypeFromParameters(partContainingResourceTypeAndUrl);
    final String url = getUrlFromParameters(partContainingResourceTypeAndUrl);

    if (resourceType == null || url == null) {
      return null;
    }

    return mapInputParams(resourceType, url);
  }

  @SuppressWarnings({"ConstantValue", "ConstantConditions", "java:S2583"})
  // Null checks needed despite @Nonnull - Jackson can deserialize nulls.
  private InputParams mapInputFieldsFromJson(final ImportManifestInput input) {
    final String resourceType = input.type();
    final String url = input.url();

    if (resourceType == null || resourceType.isBlank()) {
      throw new InvalidUserInputError("Missing type in input item.");
    }
    if (url == null || url.isBlank()) {
      throw new InvalidUserInputError("Missing url in input item.");
    }

    return mapInputParams(resourceType, url);
  }

  private InputParams mapInputParams(final String resourceType, final String url) {
    final String decodedUrl = URLDecoder.decode(url, StandardCharsets.UTF_8);
    final String convertedUrl = CacheableDatabase.convertS3ToS3aUrl(decodedUrl);
    // Normalize standard types, pass through custom types (like ViewDefinition) as-is.
    final String normalizedType = normalizeResourceType(resourceType);
    return new InputParams(normalizedType, convertedUrl);
  }

  private String normalizeResourceType(final String resourceType) {
    try {
      return ResourceType.fromCode(resourceType).toCode();
    } catch (final FHIRException e) {
      // For custom types, return the type as-is. Validation happens in
      // filterUnsupportedResourceTypes.
      return resourceType;
    }
  }

  @Nullable
  private String getUrlFromParameters(
      final List<ParametersParameterComponent> partContainingResourceTypeAndUrl) {
    return ParamUtil.extractFromPart(
            partContainingResourceTypeAndUrl,
            "url",
            UrlType.class,
            UrlType::getValue,
            false,
            Optional.empty(),
            false,
            Optional.of(new InvalidUserInputError("Missing url part in input parameter.")))
        .orElse(null);
  }

  @Nullable
  private String getResourceTypeFromParameters(
      final List<ParametersParameterComponent> partContainingResourceTypeAndUrl) {
    return ParamUtil.extractFromPart(
            partContainingResourceTypeAndUrl,
            "resourceType",
            CodeType.class,
            CodeType::getCode,
            false,
            Optional.empty(),
            false,
            Optional.of(new InvalidUserInputError("Missing resourceType part in input parameter.")))
        .orElse(null);
  }
}
