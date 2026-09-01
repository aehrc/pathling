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

package au.csiro.pathling.io;

import jakarta.annotation.Nonnull;

/**
 * Raised when a request requires the data of a resource type whose Delta table schema is behind
 * this server's encoders and has not been migrated. The message names the affected type, the
 * condition, and the available remedies, and is surfaced to API clients through an
 * OperationOutcome.
 *
 * @author John Grimes
 */
public class SchemaDriftError extends RuntimeException {

  private static final long serialVersionUID = 1L;

  @Nonnull private final String resourceCode;

  /**
   * Constructs a new SchemaDriftError for the given resource type.
   *
   * @param resourceCode the resource type whose table is drifted and unmigrated
   */
  public SchemaDriftError(@Nonnull final String resourceCode) {
    super(
        "The stored table for resource type '"
            + resourceCode
            + "' has a schema that is behind this server's encoders and cannot be queried. "
            + "Enable pathling.storage.schemaAutoMerge (or restore write access to the "
            + "warehouse) and restart, or update a resource of this type, to migrate the table.");
    this.resourceCode = resourceCode;
  }

  /**
   * Returns the resource type whose table is drifted.
   *
   * @return the resource type code
   */
  @Nonnull
  public String getResourceCode() {
    return resourceCode;
  }
}
