package au.csiro.pathling.operations.import_;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.ParamUtil;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.io.FileSystemPersistence;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.operations.OperationValidatorUtil;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
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

/**
 * @author Felix Naumann
 */
@Slf4j
@Component
public class ImportOperationValidator {

  public PreAsyncValidation.PreAsyncValidationResult<ImportRequest> validateRequest(
      @Nonnull RequestDetails requestDetails,
      @Nonnull Parameters parameters
  ) {
    boolean lenient = requestDetails.getHeaders(FhirServer.PREFER_LENIENT_HEADER.headerName())
        .stream()
        .anyMatch(FhirServer.PREFER_LENIENT_HEADER::validValue);

    Collection<ParametersParameterComponent> inputParts = ParamUtil.extractManyFromParameters(
        parameters.getParameter(),
        "input",
        ParametersParameterComponent.class,
        false,
        Collections.emptyList(),
        lenient,
        new InvalidUserInputError("The input may not be empty.")
    );
    if(lenient && inputParts.isEmpty()) {
      log.debug("There are no inputs provided in the request. Skipping because lenient=true");
    }
    Map<String, Collection<String>> input = inputParts.stream()
        .map(part -> mapInputFieldsFrom(part, lenient))
        .filter(Objects::nonNull)
        .filter(inputParams -> filterUnsupportedResourceTypes(inputParams, lenient))
        .collect(Collectors.groupingBy(
            InputParams::resourceType,
            Collectors.mapping(InputParams::url, Collectors.toCollection(ArrayList::new))
        ));
    
    SaveMode saveMode = getSaveMode(parameters, lenient);
    ImportFormat importFormat = getImportFormat(parameters, lenient);
    ImportRequest importRequest = new ImportRequest(requestDetails.getCompleteUrl(), input, saveMode, importFormat, lenient);

    List<OperationOutcome.OperationOutcomeIssueComponent> issues = Stream.of(
            OperationValidatorUtil.validateAcceptHeader(requestDetails, lenient),
            OperationValidatorUtil.validatePreferHeader(requestDetails, lenient))
        .flatMap(Collection::stream)
        .toList();
    
    return new PreAsyncValidationResult<>(importRequest, issues);
  }
  
  private boolean filterUnsupportedResourceTypes(InputParams inputParams, boolean lenient) {
    try {
      boolean supported = FhirServer.supportedResourceTypes().contains(ResourceType.fromCode(inputParams.resourceType()));
      if(!supported && !lenient) {
        throw new InvalidUserInputError("The resource type '%s' is not supported.".formatted(inputParams.resourceType()));
      }
      return supported;
    } catch (FHIRException e) {
      throw new InvalidUserInputError("Unknown resource type.", e);
    }
  }

  private static ImportFormat getImportFormat(@NotNull Parameters parameters, boolean lenient) {
    return ParamUtil.extractFromPart(
        parameters.getParameter(),
        "format",
        Coding.class,
        coding -> ImportFormat.fromCode(coding.getCode()),
        true,
        ImportFormat.NDJSON,
        lenient,
        new InvalidUserInputError("Unknown format.")
    );
  }

  private static SaveMode getSaveMode(@NotNull Parameters parameters, boolean lenient) {
    return ParamUtil.extractFromPart(
        parameters.getParameter(),
        "mode",
        Coding.class,
        coding -> SaveMode.fromCode(coding.getCode()),
        true,
        SaveMode.OVERWRITE,
        lenient,
        new InvalidUserInputError("Unknown mode.")
    );
  }

  private record InputParams(String resourceType, String url) {}

  private InputParams mapInputFieldsFrom(ParametersParameterComponent part, boolean lenient) {
    List<ParametersParameterComponent> partContainingResourceTypeAndUrl = part.getPart();
    String resourceType = getResourceType(partContainingResourceTypeAndUrl, lenient);
    if(lenient && resourceType == null) {
      log.debug("Missing resourceType part in input parameter. Skipping because lenient=true");
    }
    String url = getUrl(partContainingResourceTypeAndUrl, lenient);
    if(lenient && url == null) {
      log.debug("Missing url part in input parameter. Skipping because lenient=true");
    }
    if(resourceType == null || url == null) {
      return null;
    }
    try {
      String decodedUrl = URLDecoder.decode(url, StandardCharsets.UTF_8);
      String convertedUrl = CacheableDatabase.convertS3ToS3aUrl(decodedUrl);
      return new InputParams(ResourceType.fromCode(resourceType).toCode(), convertedUrl);
    } catch (FHIRException e) {
      throw new InvalidUserInputError("Invalid resource type %s".formatted(resourceType), e);
    }
  }

  private static String getUrl(List<ParametersParameterComponent> partContainingResourceTypeAndUrl,
      boolean lenient) {
    return ParamUtil.extractFromPart(
        partContainingResourceTypeAndUrl,
        "url",
        UrlType.class,
        UrlType::getValue,
        false,
        null,
        lenient,
        new InvalidUserInputError("Missing url part in input parameter.")
    );
  }

  private static String getResourceType(
      List<ParametersParameterComponent> partContainingResourceTypeAndUrl, boolean lenient) {
    return ParamUtil.extractFromPart(
        partContainingResourceTypeAndUrl,
        "resourceType",
        Coding.class,
        Coding::getCode,
        false,
        null,
        lenient,
        new InvalidUserInputError("Missing resourceType part in input parameter."));
  }
}
