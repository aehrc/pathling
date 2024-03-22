/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.export.fhir;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/**
 * Represents an OperationOutcome FHIR resource.
 *
 * @see <a href="https://hl7.org/fhir/r4/operationoutcome.html">OperationOutcome</a>
 */
@Value
@Builder
public class OperationOutcome {

  public static final String RESOURCE_TYPE = "OperationOutcome";

  public static final String ISSUE_TYPE_CODE_TRANSIENT = "transient";

  /**
   * Represents an issue backbone element within the OperationOutcome.
   */
  @Value
  @Builder
  static class Issue {

    /**
     * Severity of the issue: fatal | error | warning | information | success.
     */
    @Nonnull
    String severity;

    /**
     * Error or warning code.
     */
    @Nonnull
    String code;

    /**
     * Additional details about the error.
     */
    @Nonnull
    String diagnostics;
  }

  /**
   * The type of the resource. Should be "OperationOutcome".
   */
  @Builder.Default
  @Nonnull
  String resourceType = RESOURCE_TYPE;

  /**
   * A collection of issues containing detailed information about the error.
   */
  @Nonnull
  @Builder.Default
  List<Issue> issue = Collections.emptyList();

  /**
   * Parses a JSON string into an OperationOutcome instance.
   *
   * @param jsonString the JSON string to parse.
   * @return Optional with OperationOutcome if the JSON represents a valid resource of this type.
   */
  @Nonnull
  public static Optional<OperationOutcome> parse(@Nonnull final String jsonString) {
    return FhirJsonSupport.fromJson(jsonString, OperationOutcome.class)
        .filter(ou -> RESOURCE_TYPE.equals(ou.getResourceType()));
  }

  /**
   * Checks if the OperationOutcome contains (only) transient issues.
   *
   * @return true if the issue is transient, false otherwise.
   */
  public boolean isTransient() {
    return getIssue().stream().map(Issue::getCode)
    .allMatch(ISSUE_TYPE_CODE_TRANSIENT::equals);
  }
}
