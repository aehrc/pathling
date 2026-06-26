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

package au.csiro.pathling.operations.sqlquery;

import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import lombok.Value;

/**
 * A resolved leaf node wrapping a parsed {@link FhirView}. A {@code ViewDefinition} projects a
 * single FHIR resource type and never declares further dependencies, so it is always a leaf of the
 * dependency graph.
 *
 * @author John Grimes
 */
@Value
public class ResolvedViewDefinition implements ResolvedDependency {

  /** The stable canonical identity of the ViewDefinition. */
  @Nonnull String canonicalKey;

  /**
   * The parsed view, ready for execution. Its resource drives the projected-resource READ check.
   */
  @Nonnull FhirView view;
}
