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

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.library.FhirMimeTypes;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

public class Datasources {

  @Nonnull
  private final PathlingContext pathlingContext;

  @Nonnull
  private final SparkSession spark;

  public Datasources(@Nonnull final PathlingContext pathlingContext) {
    this.pathlingContext = pathlingContext;
    this.spark = pathlingContext.getSpark();
  }

  public static List<String> basenameToResource(String filename) {
    // get the basename from the filename (wihout the extension)
    // return the resource type(s) that the file is for

    return Collections.singletonList(
        ResourceType.fromCode(FilenameUtils.getBaseName(filename)).toCode());
  }

  @Nonnull
  public ReadableSource.Builder builderFromDatasource(@Nonnull final DataSource dataSource) {
    return new ReadableSource.Builder(pathlingContext).withDataSource(dataSource);
  }

  @Nonnull
  public InMemoryClientBuilder transientBuilder() {
    return new InMemoryClientBuilder(pathlingContext);
  }

  @Nonnull
  public DatabaseClientBuilder databaseBuilder() {
    return new DatabaseClientBuilder(pathlingContext);
  }

  @Nonnull
  public ReadableSource fromWarehouse(@Nonnull final String warehouseUrl) {
    return databaseBuilder().withWarehouseUrl(warehouseUrl).build();
  }

  @Nonnull
  public ReadableSource fromWarehouse(@Nonnull final String warehouseUrl,
      @Nonnull final String databaseName) {
    return databaseBuilder().withWarehouseUrl(warehouseUrl).withDatabaseName(databaseName).build();
  }


  @Nonnull
  public ReadableSource fromTextFiles(String filesGlob,
      Function<String, List<String>> filenameMapper,
      final String mimeType)
      throws URISyntaxException, IOException {
    final org.apache.hadoop.conf.Configuration hadoopConfiguration = requireNonNull(
        pathlingContext.getSpark().sparkContext()
            .hadoopConfiguration());
    final FileSystem warehouse = FileSystem.get(new URI(filesGlob), hadoopConfiguration);
    final Map<String, List<String>> filenamesByResourceTypes = Stream.of(
            warehouse.globStatus(new org.apache.hadoop.fs.Path(filesGlob)))
        .map(FileStatus::getPath)
        .map(Object::toString)
        .flatMap(filepath -> filenameMapper.apply(filepath).stream()
            .map(resourceType -> Pair.of(resourceType, filepath)))
        .collect(Collectors.groupingBy(Pair::getKey,
            Collectors.mapping(Pair::getValue, Collectors.toList())));

    InMemoryClientBuilder builder = new InMemoryClientBuilder(pathlingContext);

    filenamesByResourceTypes.forEach((resourceType, filenames) ->
        builder.withResource(resourceType,
            pathlingContext.encode(spark.read().text(filenames.toArray(String[]::new)),
                resourceType, mimeType)
        ));
    return builder.build();
  }

  @Nonnull
  public ReadableSource fromNDJsonDir(@Nonnull final String ndjsonDir)
      throws URISyntaxException, IOException {
    return fromTextFiles(Path.of(ndjsonDir, "*.ndjson").toString(), Datasources::basenameToResource,
        FhirMimeTypes.FHIR_JSON);
  }
}
