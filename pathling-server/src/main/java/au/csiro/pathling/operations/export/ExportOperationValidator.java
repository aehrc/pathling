package au.csiro.pathling.operations.export;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.config.ServerConfiguration;
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
import java.util.List;
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
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

/**
 * @author Felix Naumann
 */
@Slf4j
@Component
public class ExportOperationValidator {

  public static final Set<String> UNSUPPORTED_QUERY_PARAMS = Set.of("patient",
      "includeAssociatedData", "_typeFilter", "organizeOutputBy");
  private final FhirContext fhirContext;
  private final ServerConfiguration serverConfiguration;

  public ExportOperationValidator(FhirContext fhirContext,
      ServerConfiguration serverConfiguration) {
    this.fhirContext = fhirContext;
    this.serverConfiguration = serverConfiguration;
  }

  public PreAsyncValidation.PreAsyncValidationResult<ExportRequest> validateRequest(
      @Nonnull RequestDetails requestDetails,
      @Nonnull String outputFormat,
      @Nonnull InstantType since,
      @Nullable InstantType until,
      @Nullable List<String> type,
      @Nullable List<String> elements
  ) {
    boolean lenient = requestDetails.getHeaders(FhirServer.PREFER_LENIENT_HEADER.headerName())
        .stream()
        .anyMatch(FhirServer.PREFER_LENIENT_HEADER::validValue);

    if (type == null) {
      type = new ArrayList<>();
    }
    if (elements == null) {
      elements = new ArrayList<>();
    }

    ExportRequest exportRequest = createExportRequest(requestDetails.getCompleteUrl(), lenient,
        outputFormat, since, until, type, elements);
    List<OperationOutcome.OperationOutcomeIssueComponent> issues = Stream.of(
            validateAcceptHeader(requestDetails, lenient),
            validatePreferHeader(requestDetails, lenient),
            validateUnsupportedQueryParams(requestDetails, lenient))
        .flatMap(Collection::stream)
        .toList();
    return new PreAsyncValidation.PreAsyncValidationResult<>(exportRequest, issues);
  }

  private List<OperationOutcome.OperationOutcomeIssueComponent> validateUnsupportedQueryParams(
      RequestDetails requestDetails, boolean lenient) {
    Set<String> queryParams = requestDetails.getParameters().keySet();
    Set<String> unsupportedParams = queryParams.stream()
        .filter(UNSUPPORTED_QUERY_PARAMS::contains)
        .collect(Collectors.toSet());

    if (unsupportedParams.isEmpty()) {
      return List.of();
    }

    if (!lenient) {
      String firstUnsupported = unsupportedParams.iterator().next();
      throw new InvalidRequestException(
          "The query parameter '%s' is not supported. Either remove the query parameter or add %s: %s header."
              .formatted(firstUnsupported, FhirServer.PREFER_LENIENT_HEADER.headerName(),
                  FhirServer.PREFER_LENIENT_HEADER.preferred()));
    } else {
      return unsupportedParams.stream()
          .map(param -> new OperationOutcome.OperationOutcomeIssueComponent()
              .setCode(OperationOutcome.IssueType.INFORMATIONAL)
              .setSeverity(OperationOutcome.IssueSeverity.INFORMATION)
              .setDetails(new CodeableConcept().setText(
                  "The query parameter '%s' is not supported. Ignoring because lenient handling is enabled."
                      .formatted(param))))
          .toList();
    }
  }

  private List<OperationOutcome.OperationOutcomeIssueComponent> validateAcceptHeader(
      RequestDetails requestDetails, boolean lenient) {
    String acceptHeader = requestDetails.getHeader(FhirServer.ACCEPT_HEADER.headerName());
    boolean hasAcceptValue = FhirServer.ACCEPT_HEADER.validValue(acceptHeader);
    if (!lenient && !hasAcceptValue) {
      throw new InvalidRequestException(
          "Unknown 'Accept' header value '%s'. Only %s are allowed."
              .formatted(acceptHeader, FhirServer.ACCEPT_HEADER.acceptedHeaderValues())
      );
    }
    if (!hasAcceptValue) {
      requestDetails.addHeader(FhirServer.ACCEPT_HEADER.headerName(),
          FhirServer.ACCEPT_HEADER.preferred());
      return List.of(new OperationOutcome.OperationOutcomeIssueComponent()
          .setCode(OperationOutcome.IssueType.INFORMATIONAL)
          .setSeverity(OperationOutcome.IssueSeverity.INFORMATION)
          .setDetails(new CodeableConcept().setText(
              "Added missing header: %s %s".formatted(FhirServer.ACCEPT_HEADER.headerName(),
                  FhirServer.ACCEPT_HEADER.preferred())))
      );
    }
    return List.of();
  }

  private List<OperationOutcome.OperationOutcomeIssueComponent> validatePreferHeader(
      RequestDetails requestDetails, boolean lenient) {
    List<String> preferHeaders = requestDetails.getHeaders(
        FhirServer.PREFER_RESPOND_TYPE_HEADER.headerName());
    boolean hasRespondTypeHeaderValue = preferHeaders.stream()
        .anyMatch(FhirServer.PREFER_RESPOND_TYPE_HEADER::validValue);
    if (!lenient && !hasRespondTypeHeaderValue) {
      throw new InvalidRequestException(
          "Unknown 'Prefer' header value '%s'. Only %s is allowed."
              .formatted(preferHeaders,
                  FhirServer.PREFER_RESPOND_TYPE_HEADER.acceptedHeaderValues())
      );
    }
    if (!hasRespondTypeHeaderValue) {
      requestDetails.addHeader(FhirServer.PREFER_RESPOND_TYPE_HEADER.headerName(),
          FhirServer.PREFER_RESPOND_TYPE_HEADER.preferred());
      return List.of(new OperationOutcome.OperationOutcomeIssueComponent()
          .setCode(OperationOutcome.IssueType.INFORMATIONAL)
          .setSeverity(OperationOutcome.IssueSeverity.INFORMATION)
          .setDetails(new CodeableConcept().setText("Added missing header: %s %s".formatted(
              FhirServer.PREFER_RESPOND_TYPE_HEADER.headerName(),
              FhirServer.PREFER_RESPOND_TYPE_HEADER.preferred())))
      );
    }
    return List.of();
  }

  public ExportRequest createExportRequest(
      @Nonnull String originalRequest,
      boolean lenient,
      String outputFormat,
      InstantType since,
      InstantType until,
      List<String> type,
      List<String> elements

  ) {
    if (outputFormat == null) {
      log.debug("Missing _outputFormat detected.");
    }
    if (outputFormat != null && !FhirServer.OUTPUT_FORMAT.validValue(outputFormat)) {
      throw new InvalidRequestException("Unknown '%s' value '%s'. Only %s are allowed.".formatted(
          ExportProvider.OUTPUT_FORMAT_PARAM_NAME, outputFormat,
          FhirServer.OUTPUT_FORMAT.acceptedHeaderValues()));
    }
    List<Enumerations.ResourceType> resourceFilter = type.stream()
        .map(String::strip)
        .filter(Predicate.not(String::isEmpty))
        .flatMap(string -> Arrays.stream(string.split(",")))
        .map(code -> mapTypeQueryParam(code, lenient))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(this::filterUnauthorizedResources)
        .toList();

    List<ExportRequest.FhirElement> fhirElements = elements.stream()
        .flatMap(string -> Arrays.stream(string.split(",")))
        .map(this::mapFhirElement)
        .toList();
    return new ExportRequest(
        originalRequest,
        ExportOutputFormat.ND_JSON,
        since,
        until,
        resourceFilter,
        fhirElements,
        lenient
    );
  }

  private boolean filterUnauthorizedResources(ResourceType resourceType) {
    /*
    Check auth for resource types before checking if there are actually resources for that type.
    Otherwise, this could leak information to unauthorized users (the information 
    "no Encounter resources exist" should not be available)
     */
    if (serverConfiguration.getAuth().isEnabled()) {
      SecurityAspect.checkHasAuthority(
          PathlingAuthority.resourceAccess(AccessType.READ, resourceType));
    }
    return true; // has auth if no error from the stream chain above
  }

  private Optional<ResourceType> mapTypeQueryParam(String code, boolean lenient) {
    try {
      ResourceType resourceType = Enumerations.ResourceType.fromCode(code);
      Set<ResourceType> unsupported = FhirServer.unsupportedResourceTypes();
      if (!lenient && unsupported.contains(resourceType)) {
        throw new InvalidRequestException(
            "'_type' includes unsupported resource type '%s'. Note that '%s' are all unsupported.".formatted(
                resourceType.toCode(), unsupported));
      } else if (lenient && unsupported.contains(resourceType)) {
        return Optional.<Enumerations.ResourceType>empty();
      }
      return Optional.of(resourceType);
    } catch (FHIRException e) {
      if (lenient) {
        log.info("Failed to map '_type' value '{}' to actual FHIR resource type. Skipping.",
            code);
      } else {
        throw new InvalidRequestException(
            "Failed to map '_type' value '%s' to actual FHIR resource types.".formatted(
                code));
      }
      return Optional.empty();
    }
  }

  private ExportRequest.FhirElement mapFhirElement(@NotNull String element) {
    String[] split = element.split("\\.");
    if (split.length == 1) {
      // Only [element name] -> apply to all resources
      return new ExportRequest.FhirElement(null, element);
    } else if (split.length == 2) {
      // [resource type].[element name] -> apply to this resource type only
      validateTopLevelElement(split[0], split[1]);
      return new ExportRequest.FhirElement(ResourceType.fromCode(split[0]), split[1]);
    } else {
      throw new InvalidRequestException(
          "Failed to parse '_elements' parameter with value '%s'".formatted(element));
    }
  }

  private void validateTopLevelElement(@NotNull String resourceType, @NotNull String element)
      throws InvalidRequestException {
    try {
      RuntimeResourceDefinition resourceDef = fhirContext.getResourceDefinition(resourceType);
      if (resourceDef.getChildByName(element) == null) {
        throw new InvalidRequestException(
            "Failed to parse element '%s' for resource type '%s' in _elements.".formatted(element,
                resourceType));
      }
    } catch (DataFormatException e) {
      throw new InvalidRequestException(
          "Failed to parse resource type '%s' in _elements.".formatted(resourceType));
    }
  }
}
