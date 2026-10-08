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
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Reads FHIR JSON into this layout, from files of newline-delimited JSON, from a dataset of
 * documents, or from a dataset of bundles (decisions 70 and 84). It is obtained from {@link
 * FhirReader#json()}.
 *
 * <p>Types, cardinality and field order come from the definitions, and what the text carries
 * decides only which elements are present (FR-008, FR-012). Text that is not JSON at all fails the
 * read.
 *
 * <p>Every resource read in one call is of that one type. This is not checked: a document of
 * another type is stored as though it were of the named type, and the fields the named type lacks
 * are reported as content this layout does not store. A caller holding documents of several types
 * selects those of one type first. Bundles are the exception, because the resources of the type
 * asked for are selected from them before anything is inferred.
 *
 * <p>Nothing read here is pruned (decision 71). A structure whose only content was a primitive's id
 * and extensions is conformant and kept on purpose until M5: the primitive is stored as a null of
 * its declared type, so the structure is stored and later written as an empty object whatever other
 * conformant documents are read with it (decision 72). A metadata group in another shape, including
 * one that another document re-typed, keeps nothing. A structure emptied by content the definitions
 * do not describe or contradict is written as an empty object where anything else read with it
 * keeps its column, and left out where nothing does; a value in another document that re-types a
 * column they share empties it the same way. A repeating primitive keeps its positional nulls, and
 * until M5 they are written without the metadata they align with. Each loss is reported. Detecting
 * what is emptied is deferred to the JSON serde that the lexical form of a decimal also waits on.
 */
public final class FhirJsonReader implements FhirFormatReader {

  /** The reader option that decides what happens to a document that is not valid JSON. */
  @Nonnull private static final String READ_MODE = "mode";

  /**
   * Content that is not JSON at all fails the read, unlike content the definitions do not describe,
   * which is ignored. Reading it leniently would yield a row of nulls in place of a resource, which
   * is a silent truncation rather than an ignored field.
   */
  @Nonnull private static final String FAIL_FAST = "FAILFAST";

  @Nonnull private final SparkSession spark;

  @Nonnull private final ResourceTransformer transformer;

  @Nonnull private final FhirParsers parsers;

  FhirJsonReader(
      @Nonnull final SparkSession spark,
      @Nonnull final ResourceTransformer transformer,
      @Nonnull final FhirParsers parsers) {
    this.spark = spark;
    this.transformer = transformer;
    this.parsers = parsers;
  }

  /**
   * Reads files of newline-delimited FHIR JSON into this layout.
   *
   * @param resourceType the type of every resource the files carry
   * @param path the path to read from, which may name a file or a directory
   * @return the resources, in this layout
   * @throws IllegalArgumentException if the type is {@code Bundle} or is not a resource type
   */
  @Nonnull
  public Dataset<Row> read(@Nonnull final String resourceType, @Nonnull final String path) {
    transformer.requireStorable(resourceType);
    return transformer.toLayout(resourceType, json(spark).json(path));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Every document is a resource of the type asked for, as for files.
   */
  @Override
  @Nonnull
  public Dataset<Row> read(
      @Nonnull final String resourceType, @Nonnull final Dataset<String> documents) {
    transformer.requireStorable(resourceType);
    return transformer.toLayout(resourceType, json(documents.sparkSession()).json(documents));
  }

  @Override
  @Nonnull
  public Dataset<Row> readBundles(
      @Nonnull final String resourceType, @Nonnull final Dataset<String> bundles) {
    return read(resourceType, BundleTransformer.json(parsers).resources(resourceType, bundles));
  }

  @Nonnull
  private static DataFrameReader json(@Nonnull final SparkSession session) {
    return session.read().option(READ_MODE, FAIL_FAST);
  }
}
