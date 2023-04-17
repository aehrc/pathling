/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.library.data;

import static au.csiro.pathling.io.PersistenceScheme.departitionResult;
import static au.csiro.pathling.io.PersistenceScheme.safelyJoinPaths;

import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.PersistenceScheme.ImportMode;
import au.csiro.pathling.library.FhirMimeTypes;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.EnumerableDataSource;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * This class knows how to take an @link{EnumerableDataSource} and write it to a variety of
 * different targets.
 *
 * @author John Grimes
 */
public class DataSinks {

  @Nonnull
  private final PathlingContext pathlingContext;

  @Nonnull
  private final EnumerableDataSource dataSource;

  public DataSinks(@Nonnull final PathlingContext pathlingContext,
      @Nonnull final EnumerableDataSource dataSource) {
    this.pathlingContext = pathlingContext;
    this.dataSource = dataSource;
  }

  /**
   * Writes the data in the data source to NDJSON files, one per resource type and named using the
   * "ndjson" extension.
   *
   * @param path the directory to write the files to
   */
  public void ndjson(@Nonnull final String path) {
    ndjson(path, (resourceType) -> resourceType + ".ndjson");
  }

  /**
   * Writes the data in the data source to NDJSON files, one per resource type and named using a
   * custom file name mapper.
   *
   * @param path the directory to write the files to
   * @param fileNameMapper a function that maps a resource type to a file name
   */
  public void ndjson(@Nonnull final String path,
      @Nonnull final UnaryOperator<String> fileNameMapper) {
    for (final ResourceType resourceType : dataSource.getDefinedResources()) {
      final Dataset<String> jsonStrings = pathlingContext.decode(dataSource.read(resourceType),
          resourceType.toCode(), FhirMimeTypes.FHIR_JSON);
      final String resultUrl = safelyJoinPaths(path,
          fileNameMapper.apply(resourceType.toCode()));
      final String resultUrlPartitioned = resultUrl + ".partitioned";
      jsonStrings.coalesce(1).write().text(resultUrlPartitioned);
      departitionResult(pathlingContext.getSpark(), resultUrlPartitioned, resultUrl, "txt");
    }
  }

  /**
   * Writes the data in the data source to Parquet files, one per resource type and named using the
   * "parquet" extension.
   *
   * @param path the directory to write the files to
   */
  public void parquet(@Nonnull final String path) {
    for (final ResourceType resourceType : dataSource.getDefinedResources()) {
      final Dataset<Row> dataset = dataSource.read(resourceType);
      final String resultUrl = safelyJoinPaths(path, resourceType.toCode() + ".parquet");
      dataset.write().parquet(resultUrl);
    }
  }

  /**
   * Writes the data in the data source to Delta files, one per resource type and named using the
   * "parquet" extension. Any existing data in the Delta files will be overwritten.
   *
   * @param path the directory to write the files to
   */
  public void delta(@Nonnull final String path) {
    delta(path, ImportMode.OVERWRITE);
  }

  /**
   * Writes the data in the data source to Delta files, one per resource type and named using the
   * "parquet" extension.
   *
   * @param path the directory to write the files to
   * @param importMode the import mode to use, {@link ImportMode#OVERWRITE} will overwrite any
   * existing data, {@link ImportMode#MERGE} will merge the new data with the existing data based on
   * resource ID
   */
  public void delta(@Nonnull final String path, @Nonnull final ImportMode importMode) {
    final StorageConfiguration storageConfiguration = StorageConfiguration.builder().build();
    final Database database = new Database(storageConfiguration, pathlingContext.getSpark(),
        pathlingContext.getFhirEncoders(), path);
    for (final ResourceType resourceType : dataSource.getDefinedResources()) {
      final Dataset<Row> dataset = dataSource.read(resourceType);
      if (importMode.equals(ImportMode.OVERWRITE)) {
        database.overwrite(resourceType, dataset);
      } else if (importMode.equals(ImportMode.MERGE)) {
        database.merge(resourceType, dataset);
      } else {
        throw new IllegalArgumentException("Unsupported import mode: " + importMode);
      }
    }
  }

  /**
   * Writes the data in the data source to tables within the Spark catalog, named according to the
   * resource type.
   */
  public void tables() {
    for (final ResourceType resourceType : dataSource.getDefinedResources()) {
      final Dataset<Row> resourceDataset = dataSource.read(resourceType);
      resourceDataset.write().saveAsTable(resourceType.toCode());
    }
  }

}
