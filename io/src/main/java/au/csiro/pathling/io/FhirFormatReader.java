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
 * Reads FHIR resources written in one format into this layout, whether each document is a resource
 * or a bundle carrying resources (decision 84).
 *
 * <p>A reader for a particular format is obtained from {@link FhirReader}. How a format is parsed
 * is not part of this contract, so that it can change without changing what a caller writes.
 *
 * <p>Every read is for one resource type. {@code Bundle} is never a type that is stored, and asking
 * for it, or for a name that is not a resource type, is refused before anything is read (FR-007).
 */
public sealed interface FhirFormatReader permits FhirJsonReader, FhirXmlReader {

  /**
   * Reads a dataset of FHIR resources, one document per row, into this layout.
   *
   * <p>What happens to a document whose resource is of another type depends on the format. The XML
   * reader leaves it out, because it parses each document and so knows its type. The JSON reader
   * does not look, and stores it as though it were of the type asked for, so a caller holding JSON
   * documents of several types selects those of one type first (decisions 70 and 84).
   *
   * @param resourceType the type of the resources to read
   * @param documents the documents
   * @return the resources, in this layout
   * @throws IllegalArgumentException if the type is {@code Bundle} or is not a resource type
   */
  @Nonnull
  Dataset<Row> read(@Nonnull String resourceType, @Nonnull Dataset<String> documents);

  /**
   * Reads the resources of one type that a dataset of FHIR bundles carries, one bundle per row,
   * into this layout.
   *
   * <p>A reference that is a URN naming the full URL of another entry in the same bundle is
   * resolved to that entry's relative reference, {@code Type/id}, with {@code /_history/version}
   * where the entry's metadata names a version, and every other reference is kept as written
   * (decision 83, FR-062). Only an element the definitions type as a {@code Reference} is resolved.
   * An entry without a resource contributes nothing, and a bundle carried as the resource of an
   * entry is neither returned nor read in turn.
   *
   * <p>The resources of the type asked for are taken out of each bundle and read as the documents
   * of that type are read, so what is stored is what {@link #read} stores for the same resources,
   * and the bundle's resources of other types do not affect it. A document that is not a bundle, or
   * not well-formed in the format, fails the job that evaluates the result, naming it.
   *
   * @param resourceType the type of the resources to read
   * @param bundles the bundles
   * @return the resources of that type, in this layout
   * @throws IllegalArgumentException if the type is {@code Bundle} or is not a resource type
   */
  @Nonnull
  Dataset<Row> readBundles(@Nonnull String resourceType, @Nonnull Dataset<String> bundles);
}
