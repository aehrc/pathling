package au.csiro.pathling.operations;

import au.csiro.pathling.FhirServer;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import java.util.List;

/**
 * @author Felix Naumann
 */
public class OperationValidatorUtil {

  private OperationValidatorUtil() {}
  
  public static List<OperationOutcomeIssueComponent> validateAcceptHeader(
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
      return List.of(new OperationOutcomeIssueComponent()
          .setCode(OperationOutcome.IssueType.INFORMATIONAL)
          .setSeverity(OperationOutcome.IssueSeverity.INFORMATION)
          .setDetails(new CodeableConcept().setText(
              "Added missing header: %s %s".formatted(FhirServer.ACCEPT_HEADER.headerName(),
                  FhirServer.ACCEPT_HEADER.preferred())))
      );
    }
    return List.of();
  }

  public static List<OperationOutcomeIssueComponent> validatePreferHeader(
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
      return List.of(new OperationOutcomeIssueComponent()
          .setCode(OperationOutcome.IssueType.INFORMATIONAL)
          .setSeverity(OperationOutcome.IssueSeverity.INFORMATION)
          .setDetails(new CodeableConcept().setText("Added missing header: %s %s".formatted(
              FhirServer.PREFER_RESPOND_TYPE_HEADER.headerName(),
              FhirServer.PREFER_RESPOND_TYPE_HEADER.preferred())))
      );
    }
    return List.of();
  }
}
