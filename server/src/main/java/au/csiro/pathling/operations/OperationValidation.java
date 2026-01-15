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

package au.csiro.pathling.operations;

import au.csiro.pathling.FhirServer;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;

/**
 * Utility methods for validating FHIR operation request headers.
 *
 * @author Felix Naumann
 */
public final class OperationValidation {

  private OperationValidation() {
    // Utility class.
  }

  /**
   * Validates the Accept header in the request.
   *
   * @param requestDetails the request details containing headers
   * @param lenient if true, adds a default header instead of throwing an exception
   * @return a list of informational issues if the header was missing and added in lenient mode
   * @throws InvalidRequestException if the header is invalid and lenient mode is disabled
   */
  @Nonnull
  public static List<OperationOutcomeIssueComponent> validateAcceptHeader(
      @Nonnull final RequestDetails requestDetails, final boolean lenient) {
    final String acceptHeader = requestDetails.getHeader(FhirServer.ACCEPT_HEADER.headerName());
    final boolean hasAcceptValue = FhirServer.ACCEPT_HEADER.validValue(acceptHeader);
    if (!lenient && !hasAcceptValue) {
      throw new InvalidRequestException(
          "Unknown 'Accept' header value '%s'. Only %s are allowed."
              .formatted(acceptHeader, FhirServer.ACCEPT_HEADER.acceptedHeaderValues()));
    }
    if (!hasAcceptValue) {
      requestDetails.addHeader(
          FhirServer.ACCEPT_HEADER.headerName(), FhirServer.ACCEPT_HEADER.preferred());
      return List.of(
          new OperationOutcomeIssueComponent()
              .setCode(OperationOutcome.IssueType.INFORMATIONAL)
              .setSeverity(OperationOutcome.IssueSeverity.INFORMATION)
              .setDetails(
                  new CodeableConcept()
                      .setText(
                          "Added missing header: %s %s"
                              .formatted(
                                  FhirServer.ACCEPT_HEADER.headerName(),
                                  FhirServer.ACCEPT_HEADER.preferred()))));
    }
    return List.of();
  }

  /**
   * Validates the Prefer header in the request.
   *
   * @param requestDetails the request details containing headers
   * @param lenient if true, adds a default header instead of throwing an exception
   * @return a list of informational issues if the header was missing and added in lenient mode
   * @throws InvalidRequestException if the header is invalid and lenient mode is disabled
   */
  @Nonnull
  public static List<OperationOutcomeIssueComponent> validatePreferHeader(
      @Nonnull final RequestDetails requestDetails, final boolean lenient) {
    final List<String> preferHeaders =
        requestDetails.getHeaders(FhirServer.PREFER_RESPOND_TYPE_HEADER.headerName());
    final boolean hasRespondTypeHeaderValue =
        preferHeaders.stream().anyMatch(FhirServer.PREFER_RESPOND_TYPE_HEADER::validValue);
    if (!lenient && !hasRespondTypeHeaderValue) {
      throw new InvalidRequestException(
          "Unknown 'Prefer' header value '%s'. Only %s is allowed."
              .formatted(
                  preferHeaders, FhirServer.PREFER_RESPOND_TYPE_HEADER.acceptedHeaderValues()));
    }
    if (!hasRespondTypeHeaderValue) {
      requestDetails.addHeader(
          FhirServer.PREFER_RESPOND_TYPE_HEADER.headerName(),
          FhirServer.PREFER_RESPOND_TYPE_HEADER.preferred());
      return List.of(
          new OperationOutcomeIssueComponent()
              .setCode(OperationOutcome.IssueType.INFORMATIONAL)
              .setSeverity(OperationOutcome.IssueSeverity.INFORMATION)
              .setDetails(
                  new CodeableConcept()
                      .setText(
                          "Added missing header: %s %s"
                              .formatted(
                                  FhirServer.PREFER_RESPOND_TYPE_HEADER.headerName(),
                                  FhirServer.PREFER_RESPOND_TYPE_HEADER.preferred()))));
    }
    return List.of();
  }
}
