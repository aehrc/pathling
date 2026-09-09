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
 * A resolved leaf node for an operator-configured external table. The table is read directly from
 * its storage path by Spark rather than projected from FHIR resources, declares no further
 * dependencies and carries no version, so it is always a leaf of the dependency graph.
 *
 * @author John Grimes
 */
@Value
public class ResolvedExternalTable implements ResolvedDependency {

  /** The configured canonical URL, verbatim. External tables have no version. */
  @Nonnull String canonicalKey;

  /** The storage location of the table, used only by the Spark read. */
  @Nonnull String path;

  /** The Spark data source name ({@code delta} or {@code parquet}), used only by the Spark read. */
  @Nonnull String format;
}
