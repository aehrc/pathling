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

package au.csiro.pathling.library.io.source;

import au.csiro.fhir.export.BulkExportClient;
import au.csiro.fhir.export.BulkExportResult;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A data source that reads data from a FHIR Bulk Data endpoint. This source uses the FHIR Bulk Data
 * Export API to download resources as NDJSON files, which are then read using {@link NdjsonSource}.
 *
 * @see <a href="https://github.com/aehrc/fhir-bulk-java">FHIR Bulk Client for Java</a>
 * @see <a href="https://hl7.org/fhir/uv/bulkdata/">FHIR Bulk Data Access Implementation Guide</a>
 */
@Slf4j
public class BulkDataSource extends AbstractSource {

  @Nonnull private final NdjsonSource ndjsonSource;

  /**
   * Creates a new bulk data source using the provided client configuration.
   *
   * @param context the Pathling context
   * @param client the configured {@link BulkExportClient} that specifies the endpoint and export
   *     parameters
   * @throws RuntimeException if the export fails or the source cannot be initialized
   */
  public BulkDataSource(
      @Nonnull final PathlingContext context, @Nonnull final BulkExportClient client) {
    super(context);

    // Execute the export to the specified output directory
    final BulkExportResult result = client.export();
    log.debug("Bulk export completed: {}", result);

    // Create NdjsonSource from the exported files
    this.ndjsonSource = new NdjsonSource(context, client.getOutputDir());
  }

  private BulkDataSource(
      @Nonnull final PathlingContext context, @Nonnull final NdjsonSource ndjsonSource) {
    super(context);
    this.ndjsonSource = ndjsonSource;
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final String resourceCode) {
    return ndjsonSource.read(resourceCode);
  }

  @Nonnull
  @Override
  public Set<String> getResourceTypes() {
    return ndjsonSource.getResourceTypes();
  }

  @Override
  public QueryableDataSource map(
      @Nonnull final BiFunction<String, Dataset<Row>, Dataset<Row>> operator) {
    return new BulkDataSource(context, (NdjsonSource) ndjsonSource.map(operator));
  }

  @Override
  public @Nonnull QueryableDataSource filterByResourceType(
      @Nonnull final Predicate<String> resourceTypePredicate) {
    return new BulkDataSource(
        context, (NdjsonSource) ndjsonSource.filterByResourceType(resourceTypePredicate));
  }

  @Override
  public QueryableDataSource cache() {
    return map(Dataset::cache);
  }
}
