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

import jakarta.annotation.Nonnull;
import lombok.Value;

/**
 * Represents a reference to a ViewDefinition dependency within a SQLQuery Library resource. Each
 * reference has a label (the table alias used in the SQL) and a canonical URL pointing to the
 * ViewDefinition.
 */
@Value
public class ViewArtifactReference {

  /** The table alias used in the SQL query to reference this ViewDefinition. */
  @Nonnull String label;

  /** The canonical URL or relative reference to the ViewDefinition resource. */
  @Nonnull String canonicalUrl;
}
