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

package au.csiro.pathling.operations.bulkexport;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.operations.OperationValidation;
import au.csiro.pathling.operations.bulkexport.ExportRequest.ExportLevel;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.springframework.stereotype.Component;

/**
 * Validates export operation requests.
 *
 * @author Felix Naumann
 * @author John Grimes
 */
@Slf4j
@Component
public class ExportOperationValidator {

  /** Query parameters that are not supported by the export operation. */
  public static final Set<String> UNSUPPORTED_QUERY_PARAMS =
      Set.of("includeAssociatedData", "organizeOutputBy");

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final PatientCompartmentService patientCompartmentService;

  /**
   * Constructs a new ExportOperationValidator.
   *
   * @param fhirContext the FHIR context
   * @param serverConfiguration the server configuration
   * @param patientCompartmentService the patient compartment service
   */
  public ExportOperationValidator(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final PatientCompartmentService patientCompartmentService) {
    this.fhirContext = fhirContext;
    this.serverConfiguration = serverConfiguration;
    this.patientCompartmentService = patientCompartmentService;
  }

  /**
   * Validates a system-level export request (backwards-compatible overload without _typeFilter).
   *
   * @param requestDetails the request details
   * @param outputFormat the output format parameter
   * @param since the since parameter
   * @param until the until parameter
   * @param type the type parameter
   * @param elements the elements parameter
   * @return the pre-async validation result
   */
  public PreAsyncValidation.PreAsyncValidationResult<ExportRequest> validateRequest(
      @Nonnull final RequestDetails requestDetails,
      @Nullable final String outputFormat,
      @Nullable final InstantType since,
      @Nullable final InstantType until,
      @Nullable final List<String> type,
      @Nullable final List<String> elements) {
    return validateRequest(requestDetails, outputFormat, since, until, type, null, elements);
  }

  /**
   * Validates a system-level export request.
   *
   * @param requestDetails the request details
   * @param outputFormat the output format parameter
   * @param since the since parameter
   * @param until the until parameter
   * @param type the type parameter
   * @param typeFilter the type filter parameter
   * @param elements the elements parameter
   * @return the pre-async validation result
   */
  public PreAsyncValidation.PreAsyncValidationResult<ExportRequest> validateRequest(
      @Nonnull final RequestDetails requestDetails,
      @Nullable final String outputFormat,
      @Nullable final InstantType since,
      @Nullable final InstantType until,
      @Nullable final List<String> type,
      @Nullable final List<String> typeFilter,
      @Nullable final List<String> elements) {
    final boolean lenient =
        requestDetails.getHeaders(FhirServer.PREFER_LENIENT_HEADER.headerName()).stream()
            .anyMatch(FhirServer.PREFER_LENIENT_HEADER::validValue);

    final List<String> typeList = type != null ? type : new ArrayList<>();
    final List<String> typeFilterList = typeFilter != null ? typeFilter : new ArrayList<>();
    final List<String> elementsList = elements != null ? elements : new ArrayList<>();

    // Parse and validate _typeFilter values.
    final List<OperationOutcome.OperationOutcomeIssueComponent> typeFilterIssues =
        new ArrayList<>();
    final Map<String, List<String>> parsedTypeFilters =
        parseTypeFilters(typeFilterList, typeList, lenient, typeFilterIssues, false);

    // If _type is absent but _typeFilter is present, implicitly include referenced types.
    final List<String> effectiveTypeList;
    if (typeList.isEmpty() && !parsedTypeFilters.isEmpty()) {
      effectiveTypeList = new ArrayList<>(parsedTypeFilters.keySet());
    } else {
      effectiveTypeList = typeList;
    }

    final ExportRequest exportRequest =
        createExportRequest(
            requestDetails.getCompleteUrl(),
            requestDetails.getFhirServerBase(),
            lenient,
            outputFormat,
            since,
            until,
            effectiveTypeList,
            parsedTypeFilters,
            elementsList);
    final List<OperationOutcome.OperationOutcomeIssueComponent> issues =
        Stream.of(
                OperationValidation.validateAcceptHeader(requestDetails, lenient),
                OperationValidation.validatePreferHeader(requestDetails, lenient),
                validateUnsupportedQueryParams(requestDetails, lenient),
                typeFilterIssues)
            .flatMap(Collection::stream)
            .toList();
    return new PreAsyncValidation.PreAsyncValidationResult<>(exportRequest, issues);
  }

  /**
   * Validates a patient-level or group-level export request (backwards-compatible overload without
   * _typeFilter).
   *
   * @param requestDetails the request details
   * @param exportLevel the export level (PATIENT_TYPE, PATIENT_INSTANCE, or GROUP)
   * @param patientIds the patient IDs to export (empty for all patients)
   * @param outputFormat the output format parameter
   * @param since the since parameter
   * @param until the until parameter
   * @param type the type parameter
   * @param elements the elements parameter
   * @return the pre-async validation result
   */
  public PreAsyncValidation.PreAsyncValidationResult<ExportRequest> validatePatientExportRequest(
      @Nonnull final RequestDetails requestDetails,
      @Nonnull final ExportLevel exportLevel,
      @Nonnull final Set<String> patientIds,
      @Nullable final String outputFormat,
      @Nullable final InstantType since,
      @Nullable final InstantType until,
      @Nullable final List<String> type,
      @Nullable final List<String> elements) {
    return validatePatientExportRequest(
        requestDetails, exportLevel, patientIds, outputFormat, since, until, type, null, elements);
  }

  /**
   * Validates a patient-level or group-level export request.
   *
   * @param requestDetails the request details
   * @param exportLevel the export level (PATIENT_TYPE, PATIENT_INSTANCE, or GROUP)
   * @param patientIds the patient IDs to export (empty for all patients)
   * @param outputFormat the output format parameter
   * @param since the since parameter
   * @param until the until parameter
   * @param type the type parameter
   * @param typeFilter the type filter parameter
   * @param elements the elements parameter
   * @return the pre-async validation result
   */
  public PreAsyncValidation.PreAsyncValidationResult<ExportRequest> validatePatientExportRequest(
      @Nonnull final RequestDetails requestDetails,
      @Nonnull final ExportLevel exportLevel,
      @Nonnull final Set<String> patientIds,
      @Nullable final String outputFormat,
      @Nullable final InstantType since,
      @Nullable final InstantType until,
      @Nullable final List<String> type,
      @Nullable final List<String> typeFilter,
      @Nullable final List<String> elements) {
    final boolean lenient =
        requestDetails.getHeaders(FhirServer.PREFER_LENIENT_HEADER.headerName()).stream()
            .anyMatch(FhirServer.PREFER_LENIENT_HEADER::validValue);

    final List<String> typeList = type != null ? type : new ArrayList<>();
    final List<String> typeFilterList = typeFilter != null ? typeFilter : new ArrayList<>();
    final List<String> elementsList = elements != null ? elements : new ArrayList<>();

    // Parse and validate _typeFilter values, filtering non-compartment types for patient exports.
    final List<OperationOutcome.OperationOutcomeIssueComponent> typeFilterIssues =
        new ArrayList<>();
    final Map<String, List<String>> parsedTypeFilters =
        parseTypeFilters(typeFilterList, typeList, lenient, typeFilterIssues, true);

    // If _type is absent but _typeFilter is present, implicitly include referenced types.
    final List<String> effectiveTypeList;
    if (typeList.isEmpty() && !parsedTypeFilters.isEmpty()) {
      effectiveTypeList = new ArrayList<>(parsedTypeFilters.keySet());
    } else {
      effectiveTypeList = typeList;
    }

    final ExportRequest exportRequest =
        createPatientExportRequest(
            requestDetails.getCompleteUrl(),
            requestDetails.getFhirServerBase(),
            lenient,
            outputFormat,
            since,
            until,
            effectiveTypeList,
            parsedTypeFilters,
            elementsList,
            exportLevel,
            patientIds);

    final List<OperationOutcome.OperationOutcomeIssueComponent> issues =
        Stream.of(
                OperationValidation.validateAcceptHeader(requestDetails, lenient),
                OperationValidation.validatePreferHeader(requestDetails, lenient),
                validateUnsupportedQueryParams(requestDetails, lenient),
                typeFilterIssues)
            .flatMap(Collection::stream)
            .toList();
    return new PreAsyncValidation.PreAsyncValidationResult<>(exportRequest, issues);
  }

  private List<OperationOutcome.OperationOutcomeIssueComponent> validateUnsupportedQueryParams(
      final RequestDetails requestDetails, final boolean lenient) {
    final Set<String> queryParams = requestDetails.getParameters().keySet();
    final Set<String> unsupportedParams =
        queryParams.stream().filter(UNSUPPORTED_QUERY_PARAMS::contains).collect(Collectors.toSet());

    if (unsupportedParams.isEmpty()) {
      return List.of();
    }

    if (!lenient) {
      final String firstUnsupported = unsupportedParams.iterator().next();
      throw new InvalidRequestException(
          ("The query parameter '%s' is not supported. Either remove the query parameter or add "
                  + "%s: %s header.")
              .formatted(
                  firstUnsupported,
                  FhirServer.PREFER_LENIENT_HEADER.headerName(),
                  FhirServer.PREFER_LENIENT_HEADER.preferred()));
    } else {
      return unsupportedParams.stream()
          .map(
              param ->
                  new OperationOutcome.OperationOutcomeIssueComponent()
                      .setCode(OperationOutcome.IssueType.INFORMATIONAL)
                      .setSeverity(OperationOutcome.IssueSeverity.INFORMATION)
                      .setDetails(
                          new CodeableConcept()
                              .setText(
                                  ("The query parameter '%s' is not supported. Ignoring because "
                                          + "lenient handling is enabled.")
                                      .formatted(param))))
          .toList();
    }
  }

  /**
   * Parses and validates _typeFilter values, returning a map keyed by resource type code.
   *
   * @param typeFilterList the raw _typeFilter parameter values
   * @param typeList the _type parameter values (for consistency checking)
   * @param lenient whether lenient handling is enabled
   * @param issues mutable list to collect informational issues for lenient mode
   * @param filterNonCompartment whether to silently filter out non-compartment resource types
   * @return a map of resource type code to list of search query strings
   */
  @Nonnull
  private Map<String, List<String>> parseTypeFilters(
      @Nonnull final List<String> typeFilterList,
      @Nonnull final List<String> typeList,
      final boolean lenient,
      @Nonnull final List<OperationOutcome.OperationOutcomeIssueComponent> issues,
      final boolean filterNonCompartment) {
    if (typeFilterList.isEmpty()) {
      return Map.of();
    }

    // Resolve the effective _type set for consistency checking.
    final Set<String> typeSet =
        typeList.stream()
            .map(String::strip)
            .filter(Predicate.not(String::isEmpty))
            .flatMap(string -> Arrays.stream(string.split(",")))
            .collect(Collectors.toSet());

    final Map<String, List<String>> result = new java.util.LinkedHashMap<>();

    for (final String filterValue : typeFilterList) {
      // Split on the first '?' to separate resource type from search query.
      final int questionMarkIndex = filterValue.indexOf('?');
      if (questionMarkIndex < 0) {
        throw new InvalidRequestException(
            "_typeFilter value '%s' has invalid format. Expected format: [ResourceType]?[search-params]"
                .formatted(filterValue));
      }

      final String resourceTypeCode = filterValue.substring(0, questionMarkIndex);
      final String searchQuery = filterValue.substring(questionMarkIndex + 1);

      if (searchQuery.isEmpty()) {
        throw new InvalidRequestException(
            "_typeFilter value '%s' has an empty search query.".formatted(filterValue));
      }

      // Validate the resource type.
      final Optional<String> validatedType = mapTypeQueryParam(resourceTypeCode, lenient);
      if (validatedType.isEmpty()) {
        // In lenient mode, mapTypeQueryParam returns empty and logs. Add an informational issue.
        issues.add(
            new OperationOutcome.OperationOutcomeIssueComponent()
                .setCode(OperationOutcome.IssueType.INFORMATIONAL)
                .setSeverity(OperationOutcome.IssueSeverity.INFORMATION)
                .setDetails(
                    new CodeableConcept()
                        .setText(
                            "_typeFilter references unknown resource type '%s'. Ignoring because lenient handling is enabled."
                                .formatted(resourceTypeCode))));
        continue;
      }

      final String resolvedType = validatedType.get();

      // For patient/group exports, filter out non-compartment resource types silently.
      if (filterNonCompartment && !patientCompartmentService.isInPatientCompartment(resolvedType)) {
        log.info(
            "_typeFilter resource type '{}' is not in the Patient compartment. Ignoring.",
            resolvedType);
        continue;
      }

      // Check consistency with _type parameter if _type was provided.
      if (!typeSet.isEmpty() && !typeSet.contains(resolvedType)) {
        if (!lenient) {
          throw new InvalidRequestException(
              "_typeFilter references resource type '%s' which is not included in _type parameter."
                  .formatted(resolvedType));
        } else {
          issues.add(
              new OperationOutcome.OperationOutcomeIssueComponent()
                  .setCode(OperationOutcome.IssueType.INFORMATIONAL)
                  .setSeverity(OperationOutcome.IssueSeverity.INFORMATION)
                  .setDetails(
                      new CodeableConcept()
                          .setText(
                              "_typeFilter references resource type '%s' which is not included in _type. Ignoring because lenient handling is enabled."
                                  .formatted(resolvedType))));
          continue;
        }
      }

      result.computeIfAbsent(resolvedType, k -> new ArrayList<>()).add(searchQuery);
    }

    return result;
  }

  /**
   * Creates an ExportRequest for system-level exports.
   *
   * @param originalRequest the original request URL
   * @param serverBaseUrl the FHIR server base URL
   * @param lenient whether lenient handling is enabled
   * @param outputFormat the output format
   * @param since the since parameter
   * @param until the until parameter
   * @param type the type parameter
   * @param typeFilters the parsed type filters
   * @param elements the elements parameter
   * @return the export request
   */
  public ExportRequest createExportRequest(
      @Nonnull final String originalRequest,
      @Nonnull final String serverBaseUrl,
      final boolean lenient,
      @Nullable final String outputFormat,
      @Nullable final InstantType since,
      @Nullable final InstantType until,
      @Nonnull final List<String> type,
      @Nonnull final Map<String, List<String>> typeFilters,
      @Nonnull final List<String> elements) {
    if (outputFormat == null) {
      log.debug("No _outputFormat specified, defaulting to ndjson.");
    }
    final ExportOutputFormat resolvedFormat = ExportOutputFormat.fromParam(outputFormat);
    if (resolvedFormat == null) {
      throw new InvalidRequestException(
          "Unknown '%s' value '%s'. Only %s are allowed."
              .formatted(
                  SystemExportProvider.OUTPUT_FORMAT_PARAM_NAME,
                  outputFormat,
                  FhirServer.OUTPUT_FORMAT.acceptedHeaderValues()));
    }
    final List<String> resourceFilter =
        type.stream()
            .map(String::strip)
            .filter(Predicate.not(String::isEmpty))
            .flatMap(string -> Arrays.stream(string.split(",")))
            .map(code -> mapTypeQueryParam(code, lenient))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(this::filterUnauthorizedResources)
            .toList();

    checkForDuplicateResourceTypes(resourceFilter);

    final List<ExportRequest.FhirElement> fhirElements =
        elements.stream()
            .flatMap(string -> Arrays.stream(string.split(",")))
            .map(this::mapFhirElement)
            .toList();
    return new ExportRequest(
        originalRequest,
        serverBaseUrl,
        resolvedFormat,
        since,
        until,
        resourceFilter,
        typeFilters,
        fhirElements,
        lenient,
        ExportLevel.SYSTEM,
        Set.of());
  }

  /**
   * Creates an ExportRequest for patient-level or group-level exports.
   *
   * @param originalRequest the original request URL
   * @param serverBaseUrl the FHIR server base URL
   * @param lenient whether lenient handling is enabled
   * @param outputFormat the output format
   * @param since the since parameter
   * @param until the until parameter
   * @param type the type parameter
   * @param typeFilters the parsed type filters
   * @param elements the elements parameter
   * @param exportLevel the export level
   * @param patientIds the patient IDs to export
   * @return the export request
   */
  public ExportRequest createPatientExportRequest(
      @Nonnull final String originalRequest,
      @Nonnull final String serverBaseUrl,
      final boolean lenient,
      @Nullable final String outputFormat,
      @Nullable final InstantType since,
      @Nullable final InstantType until,
      @Nonnull final List<String> type,
      @Nonnull final Map<String, List<String>> typeFilters,
      @Nonnull final List<String> elements,
      @Nonnull final ExportLevel exportLevel,
      @Nonnull final Set<String> patientIds) {
    if (outputFormat == null) {
      log.debug("No _outputFormat specified for patient export, defaulting to ndjson.");
    }
    final ExportOutputFormat resolvedFormat = ExportOutputFormat.fromParam(outputFormat);
    if (resolvedFormat == null) {
      throw new InvalidRequestException(
          "Unknown '%s' value '%s'. Only %s are allowed."
              .formatted(
                  SystemExportProvider.OUTPUT_FORMAT_PARAM_NAME,
                  outputFormat,
                  FhirServer.OUTPUT_FORMAT.acceptedHeaderValues()));
    }

    // For patient-level exports, filter out non-compartment resource types (silently).
    final List<String> resourceFilter =
        type.stream()
            .map(String::strip)
            .filter(Predicate.not(String::isEmpty))
            .flatMap(string -> Arrays.stream(string.split(",")))
            .map(code -> mapTypeQueryParam(code, lenient))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(this::filterUnauthorizedResources)
            .filter(this::filterNonCompartmentResources)
            .toList();

    checkForDuplicateResourceTypes(resourceFilter);

    final List<ExportRequest.FhirElement> fhirElements =
        elements.stream()
            .flatMap(string -> Arrays.stream(string.split(",")))
            .map(this::mapFhirElement)
            .toList();

    return new ExportRequest(
        originalRequest,
        serverBaseUrl,
        resolvedFormat,
        since,
        until,
        resourceFilter,
        typeFilters,
        fhirElements,
        lenient,
        exportLevel,
        patientIds);
  }

  /**
   * Filters out resource types that are not in the Patient compartment. Non-compartment types are
   * silently ignored as per the lenient behaviour decision.
   *
   * @param resourceTypeCode the resource type code to check
   * @return true if the resource type should be included
   */
  private boolean filterNonCompartmentResources(@Nonnull final String resourceTypeCode) {
    final boolean inCompartment =
        patientCompartmentService.isInPatientCompartment(resourceTypeCode);
    if (!inCompartment) {
      log.info("Resource type '{}' is not in the Patient compartment. Ignoring.", resourceTypeCode);
    }
    return inCompartment;
  }

  private boolean filterUnauthorizedResources(@Nonnull final String resourceTypeCode) {
    // Check auth for resource types before checking if there are actually resources for that type.
    // Otherwise, this could leak information to unauthorised users (the information "no Encounter
    // resources exist" should not be available).
    if (serverConfiguration.getAuth().isEnabled()) {
      SecurityAspect.checkHasAuthority(
          PathlingAuthority.resourceAccess(AccessType.READ, resourceTypeCode));
    }
    return true; // has auth if no error from the stream chain above
  }

  private Optional<String> mapTypeQueryParam(final String code, final boolean lenient) {
    // Check if this is a custom resource type first.
    if (FhirServer.isCustomResourceType(code)) {
      return Optional.of(code);
    }

    try {
      final ResourceType resourceType = Enumerations.ResourceType.fromCode(code);
      final Set<ResourceType> unsupported = FhirServer.unsupportedResourceTypes();
      if (!lenient && unsupported.contains(resourceType)) {
        throw new InvalidRequestException(
            "'_type' includes unsupported resource type '%s'. Note that '%s' are all unsupported."
                .formatted(resourceType.toCode(), unsupported));
      } else if (lenient && unsupported.contains(resourceType)) {
        return Optional.empty();
      }
      return Optional.of(resourceType.toCode());
    } catch (final FHIRException e) {
      if (lenient) {
        log.info("Failed to map '_type' value '{}' to actual FHIR resource type. Skipping.", code);
      } else {
        throw new InvalidRequestException(
            "Failed to map '_type' value '%s' to actual FHIR resource types.".formatted(code));
      }
      return Optional.empty();
    }
  }

  private ExportRequest.FhirElement mapFhirElement(@Nonnull final String element) {
    final String[] split = element.split("\\.");
    if (split.length == 1) {
      // Only [element name] -> apply to all resources.
      return new ExportRequest.FhirElement(null, element);
    } else if (split.length == 2) {
      // [resource type].[element name] -> apply to this resource type only.
      validateTopLevelElement(split[0], split[1]);
      return new ExportRequest.FhirElement(split[0], split[1]);
    } else {
      throw new InvalidRequestException(
          "Failed to parse '_elements' parameter with value '%s'".formatted(element));
    }
  }

  private void validateTopLevelElement(
      @Nonnull final String resourceType, @Nonnull final String element)
      throws InvalidRequestException {
    try {
      final RuntimeResourceDefinition resourceDef = fhirContext.getResourceDefinition(resourceType);
      if (resourceDef.getChildByName(element) == null) {
        throw new InvalidRequestException(
            "Failed to parse element '%s' for resource type '%s' in _elements."
                .formatted(element, resourceType));
      }
    } catch (final DataFormatException e) {
      throw new InvalidRequestException(
          "Failed to parse resource type '%s' in _elements.".formatted(resourceType));
    }
  }

  /**
   * Checks for duplicate resource types in the provided list and throws an exception if any are
   * found.
   *
   * @param resourceTypes the list of resource types to check
   * @throws InvalidRequestException if duplicate resource types are detected
   */
  private void checkForDuplicateResourceTypes(@Nonnull final List<String> resourceTypes) {
    final Set<String> seen = new HashSet<>();
    for (final String type : resourceTypes) {
      if (!seen.add(type)) {
        throw new InvalidRequestException(
            "Duplicate resource type '%s' in _type parameter.".formatted(type));
      }
    }
  }
}
