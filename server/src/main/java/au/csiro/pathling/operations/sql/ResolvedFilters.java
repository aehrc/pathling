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

package au.csiro.pathling.operations.sql;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;

/**
 * The resolved {@code patient}, {@code group} and {@code _since} filters of a {@code $sql-run} or
 * {@code $sql-export} request, together with any issues raised while resolving them.
 *
 * <p>Issues are carried rather than thrown so the caller can combine them with a subject failure
 * into one {@code OperationOutcome}, as the contract requires: a request naming both an
 * unresolvable subject and an unresolvable filter reports both problems at once, and at export
 * kick-off every problem in the request is reported together.
 *
 * @param patientIds the patient logical ids the request restricts to, from both {@code patient} and
 *     the members of each {@code group}; empty when no compartment filter applies
 * @param since the {@code _since} filter, or null when none was supplied
 * @param issues the issues raised while resolving the filters, in request order
 * @author John Grimes
 */
public record ResolvedFilters(
    @Nonnull Set<String> patientIds,
    @Nullable InstantType since,
    @Nonnull List<OperationOutcomeIssueComponent> issues) {

  /**
   * Indicates whether any filter value failed to resolve.
   *
   * @return true if there are issues to report
   */
  public boolean hasIssues() {
    return !issues.isEmpty();
  }
}
