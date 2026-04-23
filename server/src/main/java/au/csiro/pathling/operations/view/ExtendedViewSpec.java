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

package au.csiro.pathling.operations.view;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import lombok.Data;

/**
 * Represents a parsed view specification from the extended ViewDefinition run operation parameters.
 *
 * <p>Each view spec defines a single resource query with optional join information for
 * cross-resource queries. The {@code joinTo} and {@code joinOn} fields enable joining this view's
 * results to another resource type (typically the anchor type).
 *
 * @author jkiddo
 * @see ExtendedViewDefinitionRunProvider
 */
@Data
public class ExtendedViewSpec {

  /** The FHIR resource type for this view (e.g., "Patient", "MedicationStatement"). */
  @Nonnull private final String resourceType;

  /** FHIRPath expressions defining the columns to select. */
  @Nonnull private final List<String> selectExpressions;

  /** FHIRPath filter expressions applied with AND logic. */
  @Nonnull private final List<String> whereExpressions;

  /** The resource type to join this view's results to, or null if no join is needed. */
  @Nullable private final String joinTo;

  /** The FHIRPath expression for the join column (e.g., "subject.reference"), or null. */
  @Nullable private final String joinOn;
}
