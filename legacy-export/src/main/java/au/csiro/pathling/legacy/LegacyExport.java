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
package au.csiro.pathling.legacy;

import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Converts a table in the legacy layout, written by the Pathling FHIR encoders before the Parquet
 * on FHIR layout, to FHIR NDJSON, from which it can be re-imported into the Parquet on FHIR layout
 * (FR-064).
 *
 * <pre>{@code
 * LegacyExport export = LegacyExport.of(spark);
 * export.export("Patient", "/warehouse/Patient.parquet", LegacyTableFormat.DELTA, "/export");
 * }</pre>
 *
 * <p>A table holds one resource type, which the caller names, because the legacy layout does not
 * record it. Each resource is decoded with the legacy encoder and serialised as FHIR JSON, one
 * document per line. The conversion is lossy: see the package documentation for what is lost.
 *
 * @author Piotr Szul
 */
@Slf4j
public final class LegacyExport {

  @Nonnull private final SparkSession spark;

  private LegacyExport(@Nonnull final SparkSession spark) {
    this.spark = spark;
  }

  /**
   * Returns an export that reads and writes with a Spark session. The session needs Delta Lake
   * configured only to read Delta tables.
   *
   * @param spark the session
   * @return the export
   */
  @Nonnull
  public static LegacyExport of(@Nonnull final SparkSession spark) {
    return new LegacyExport(spark);
  }

  /**
   * Reads a table, in any layout, as a dataset of rows.
   *
   * @param path the location of the table
   * @param format the format the table is stored in
   * @return the table
   * @throws LegacyExportException if the table cannot be read
   */
  @Nonnull
  public Dataset<Row> read(@Nonnull final String path, @Nonnull final LegacyTableFormat format) {
    try {
      return spark.read().format(format.getName()).load(path);
    } catch (final Exception e) {
      // Spark signals a missing or unreadable table with an analysis exception, which Java sees as
      // checked but cannot name in a catch clause, because the Scala method does not declare it.
      throw new LegacyExportException(
          "The table at " + path + " cannot be read as " + format.getName() + ": " + e.getMessage(),
          e);
    }
  }

  /**
   * Converts a legacy-layout table to FHIR JSON, one document per row.
   *
   * @param resourceType the resource type the table holds
   * @param table the table
   * @return the documents, each a single line of FHIR JSON
   * @throws LegacyExportException if the table is not in the legacy layout, or holds a column the
   *     legacy encoder for the resource type would not read
   */
  @Nonnull
  public Dataset<String> toNdjson(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> table) {
    LegacyLayout.check(resourceType, table.schema());
    return table
        .as(LegacyDecoder.encoderFor(resourceType, table.schema()))
        .mapPartitions(new ResourceJsonEncoding(), Encoders.STRING());
  }

  /**
   * Exports a legacy-layout table to files of FHIR NDJSON in a directory, named {@code
   * <type>.<part>.ndjson} so that the NDJSON source reads them as the resource type. The directory
   * may hold the exports of other resource types, but not of this one.
   *
   * @param resourceType the resource type the table holds
   * @param source the location of the table
   * @param format the format the table is stored in
   * @param outputDirectory the directory to write into, which is created if it does not exist
   * @return the paths of the files written
   * @throws LegacyExportException if the table cannot be read, is not in the legacy layout or holds
   *     a column the legacy encoder would not read, or if the directory already holds files of the
   *     resource type
   */
  @Nonnull
  public List<String> export(
      @Nonnull final String resourceType,
      @Nonnull final String source,
      @Nonnull final LegacyTableFormat format,
      @Nonnull final String outputDirectory) {
    final Dataset<String> documents = toNdjson(resourceType, read(source, format));
    final List<String> written = NdjsonOutput.write(documents, resourceType, outputDirectory);
    log.debug(
        "Exported {} from {} to {} files in {}",
        resourceType,
        source,
        written.size(),
        outputDirectory);
    return written;
  }
}
