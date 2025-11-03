package au.csiro.pathling.operations.import_;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.ParamUtil;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.operations.OperationValidatorUtil;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
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
      @Nonnull final RequestDetails requestDetails,
      @Nonnull final Parameters parameters
  ) {
    // Extract inputSource parameter (required by SMART spec).
    final String inputSource = Objects.requireNonNull(
        ParamUtil.extractFromPart(
            parameters.getParameter(),
            "inputSource",
            StringType.class,
            StringType::getValue,
            false,
            null,
            false,
            new InvalidUserInputError("Missing required parameter: inputSource")
        ),
        "inputSource must not be null"
    );

    // Extract input parameters.
    final Collection<ParametersParameterComponent> inputParts = ParamUtil.extractManyFromParameters(
        parameters.getParameter(),
        "input",
        ParametersParameterComponent.class,
        false,
        Collections.emptyList(),
        false,
        new InvalidUserInputError("The input may not be empty.")
    );

    final Map<String, Collection<String>> input = inputParts.stream()
        .map(this::mapInputFieldsFromParameters)
        .filter(Objects::nonNull)
        .filter(this::filterUnsupportedResourceTypes)
        .collect(Collectors.groupingBy(
            InputParams::resourceType,
            Collectors.mapping(InputParams::url, Collectors.toCollection(ArrayList::new))
        ));

    final SaveMode saveMode = getSaveModeFromParameters(parameters);
    final ImportFormat importFormat = getImportFormatFromParameters(parameters);
    final ImportRequest importRequest = new ImportRequest(
        requestDetails.getCompleteUrl(),
        inputSource,
        input,
        saveMode,
        importFormat
    );

    final List<OperationOutcome.OperationOutcomeIssueComponent> issues = Stream.of(
            OperationValidatorUtil.validateAcceptHeader(requestDetails, false),
            OperationValidatorUtil.validatePreferHeader(requestDetails, false))
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
  @SuppressWarnings("ConstantValue")  // Null checks needed despite @Nonnull - Jackson can deserialize nulls.
  public PreAsyncValidation.PreAsyncValidationResult<ImportRequest> validateJsonRequest(
      @Nonnull final RequestDetails requestDetails,
      @Nonnull final ImportManifest manifest
  ) {
    // Validate required fields (null checks needed because Jackson may deserialize nulls).
    if (manifest.inputFormat() == null || manifest.inputFormat().isBlank()) {
      throw new InvalidUserInputError("Missing required field: inputFormat");
    }
    if (manifest.inputSource() == null || manifest.inputSource().isBlank()) {
      throw new InvalidUserInputError("Missing required field: inputSource");
    }
    if (manifest.input() == null || manifest.input().isEmpty()) {
      throw new InvalidUserInputError("The input may not be empty.");
    }

    // Parse and validate input array.
    final Map<String, Collection<String>> input = manifest.input().stream()
        .map(this::mapInputFieldsFromJson)
        .filter(this::filterUnsupportedResourceTypes)
        .collect(Collectors.groupingBy(
            InputParams::resourceType,
            Collectors.mapping(InputParams::url, Collectors.toCollection(ArrayList::new))
        ));

    // Parse format and mode.
    final ImportFormat importFormat = parseImportFormat(manifest.inputFormat());
    final SaveMode saveMode = manifest.mode() != null && !manifest.mode().isBlank()
                              ? SaveMode.fromCode(manifest.mode())
                              : SaveMode.OVERWRITE; // Default to OVERWRITE.

    final ImportRequest importRequest = new ImportRequest(
        requestDetails.getCompleteUrl(),
        manifest.inputSource(),
        input,
        saveMode,
        importFormat
    );

    final List<OperationOutcome.OperationOutcomeIssueComponent> issues = Stream.of(
            OperationValidatorUtil.validateAcceptHeader(requestDetails, false),
            OperationValidatorUtil.validatePreferHeader(requestDetails, false))
        .flatMap(Collection::stream)
        .toList();

    return new PreAsyncValidationResult<>(importRequest, issues);
  }

  private boolean filterUnsupportedResourceTypes(final InputParams inputParams) {
    try {
      final boolean supported = FhirServer.supportedResourceTypes()
          .contains(ResourceType.fromCode(inputParams.resourceType()));
      if (!supported) {
        throw new InvalidUserInputError(
            "The resource type '%s' is not supported.".formatted(inputParams.resourceType()));
      }
      return true;
    } catch (final FHIRException e) {
      throw new InvalidUserInputError("Unknown resource type.", e);
    }
  }

  private ImportFormat getImportFormatFromParameters(final Parameters parameters) {
    return ParamUtil.extractFromPart(
        parameters.getParameter(),
        "inputFormat",
        Coding.class,
        coding -> parseImportFormat(coding.getCode()),
        true,
        ImportFormat.NDJSON, // Default to NDJSON.
        false,
        new InvalidUserInputError("Unknown format.")
    );
  }

  private SaveMode getSaveModeFromParameters(final Parameters parameters) {
    return ParamUtil.extractFromPart(
        parameters.getParameter(),
        "mode",
        Coding.class,
        coding -> SaveMode.fromCode(coding.getCode()),
        true,
        SaveMode.OVERWRITE, // Default to OVERWRITE.
        false,
        new InvalidUserInputError("Unknown mode.")
    );
  }

  /**
   * Parses an import format string, supporting both simple codes (e.g., "ndjson") and MIME types
   * (e.g., "application/fhir+ndjson").
   *
   * @param formatString the format string
   * @return the ImportFormat
   */
  private ImportFormat parseImportFormat(final String formatString) {
    if (formatString == null || formatString.isBlank()) {
      return ImportFormat.NDJSON; // Default.
    }
    // Try direct code match first.
    try {
      return ImportFormat.fromCode(formatString);
    } catch (final IllegalArgumentException e) {
      // Try MIME type mapping.
      return switch (formatString.toLowerCase()) {
        case "application/fhir+ndjson" -> ImportFormat.NDJSON;
        case "application/parquet" -> ImportFormat.PARQUET;
        case "application/delta" -> ImportFormat.DELTA;
        default -> throw new InvalidUserInputError("Unsupported format: " + formatString);
      };
    }
  }

  private record InputParams(
      String resourceType,
      String url
  ) {

  }

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

  @SuppressWarnings("ConstantValue")  // Null checks needed despite @Nonnull - Jackson can deserialize nulls.
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
    try {
      final String decodedUrl = URLDecoder.decode(url, StandardCharsets.UTF_8);
      final String convertedUrl = CacheableDatabase.convertS3ToS3aUrl(decodedUrl);
      return new InputParams(ResourceType.fromCode(resourceType).toCode(), convertedUrl);
    } catch (final FHIRException e) {
      throw new InvalidUserInputError("Invalid resource type %s".formatted(resourceType), e);
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
        null,
        false,
        new InvalidUserInputError("Missing url part in input parameter.")
    );
  }

  @Nullable
  private String getResourceTypeFromParameters(
      final List<ParametersParameterComponent> partContainingResourceTypeAndUrl) {
    return ParamUtil.extractFromPart(
        partContainingResourceTypeAndUrl,
        "resourceType",
        Coding.class,
        Coding::getCode,
        false,
        null,
        false,
        new InvalidUserInputError("Missing resourceType part in input parameter."));
  }
}
