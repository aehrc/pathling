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

  /**
   * An external table's rows come from the location it is pointed at, read as the configured
   * format, both of which the operator can change while the canonical URL stays as it was. Each
   * component is length-prefixed (see {@link ResolvedDependency#encode(String)}) so that a path
   * containing ':' cannot shift the boundary between the format and the location.
   *
   * @return the content description
   */
  @Override
  @Nonnull
  public String describeContent() {
    return "external-table:"
        + ResolvedDependency.encode(format)
        + ':'
        + ResolvedDependency.encode(path);
  }
}
