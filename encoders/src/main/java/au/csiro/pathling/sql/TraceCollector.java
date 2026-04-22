/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
package au.csiro.pathling.sql;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Collects trace data produced by the FHIRPath {@code trace()} function during expression
 * evaluation. Implementations determine how entries are stored and retrieved.
 *
 * @author John Grimes
 */
public interface TraceCollector {

  /**
   * Adds a trace entry.
   *
   * @param label the trace label (the {@code name} argument to {@code trace()})
   * @param fhirType the FHIR type code of the traced collection (e.g., {@code "HumanName"}, {@code
   *     "string"})
   * @param value the traced value, converted from Spark internal representation to an external
   *     Scala type (e.g., {@link org.apache.spark.sql.Row} for structs, {@link String} for strings)
   */
  void add(@Nonnull String label, @Nonnull String fhirType, @Nullable Object value);
}
