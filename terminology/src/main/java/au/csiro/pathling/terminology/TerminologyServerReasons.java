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

package au.csiro.pathling.terminology;

import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome;

/**
 * Builds the human-readable reasons that describe a failed request to a terminology server, so that
 * every operation reports the same failure in the same words.
 *
 * @author John Grimes
 */
public final class TerminologyServerReasons {

  private TerminologyServerReasons() {
    // Static helper.
  }

  /**
   * Describes a server response that is not a success, carrying the reason given by the response's
   * OperationOutcome where it has one and the HTTP status otherwise.
   *
   * @param e the response exception
   * @return the reason, of the form {@code the terminology server returned HTTP <status>: <reason>}
   */
  @Nonnull
  public static String httpFailure(@Nonnull final BaseServerResponseException e) {
    final String reasons = reasonsOf(e.getOperationOutcome());
    return "the terminology server returned HTTP "
        + e.getStatusCode()
        + ": "
        + (reasons.isEmpty() ? e.getMessage() : reasons);
  }

  /**
   * Describes a server that could not be reached.
   *
   * @param serverUrl the configured base URL of the server
   * @return the reason, of the form {@code terminology server <serverUrl> could not be reached}
   */
  @Nonnull
  public static String unreachable(@Nonnull final String serverUrl) {
    return "terminology server " + serverUrl + " could not be reached";
  }

  /**
   * Joins the reasons of an OperationOutcome's issues, or returns an empty string where there is no
   * outcome or no issue gives a reason. An issue's reason is its {@code diagnostics}, or where it
   * has none, its {@code details.text}: servers such as Ontoserver report the reason only in the
   * latter.
   */
  @Nonnull
  private static String reasonsOf(@Nullable final IBaseOperationOutcome outcome) {
    if (!(outcome instanceof final OperationOutcome operationOutcome)) {
      return "";
    }
    return operationOutcome.getIssue().stream()
        .map(TerminologyServerReasons::reasonOf)
        .flatMap(Optional::stream)
        .collect(Collectors.joining("; "));
  }

  /** Returns an issue's diagnostics, falling back to its details text. */
  @Nonnull
  private static Optional<String> reasonOf(
      @Nonnull final OperationOutcome.OperationOutcomeIssueComponent issue) {
    if (issue.hasDiagnostics()) {
      return Optional.of(issue.getDiagnostics());
    }
    return issue.hasDetails() && issue.getDetails().hasText()
        ? Optional.of(issue.getDetails().getText())
        : Optional.empty();
  }
}
