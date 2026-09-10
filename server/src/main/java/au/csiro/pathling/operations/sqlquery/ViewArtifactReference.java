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
 * Represents a dependency reference within a SQLQuery or SQLView Library resource (a {@code
 * relatedArtifact} of type {@code depends-on}). Each reference has a label (the table name used in
 * the SQL) and the canonical URL of the ViewDefinition or SQLView that backs that table. The
 * canonical URL is matched against the referenced resource's {@code url} element; the logical id
 * plays no part in resolution.
 *
 * @author John Grimes
 */
@Value
public class ViewArtifactReference {

  /** The table name used in the SQL query to reference this dependency. */
  @Nonnull String label;

  /**
   * The absolute canonical URL of the referenced ViewDefinition or SQLView, optionally suffixed
   * with {@code |version}.
   */
  @Nonnull String canonicalUrl;
}
