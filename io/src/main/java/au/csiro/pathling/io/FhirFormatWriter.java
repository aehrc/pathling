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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Writes resources stored in this layout as FHIR documents in one format, one per resource
 * (decision 84).
 *
 * <p>A writer for a particular format is obtained from {@link FhirWriter}. How a format is written
 * is not part of this contract, so that it can change without changing what a caller writes.
 */
public sealed interface FhirFormatWriter permits FhirJsonWriter, FhirXmlWriter {

  /**
   * Returns one FHIR document per stored resource.
   *
   * @param resourceType the type of the resources the dataset carries
   * @param stored the resources, in this layout
   * @return the documents
   */
  @Nonnull
  Dataset<String> write(@Nonnull String resourceType, @Nonnull Dataset<Row> stored);
}
