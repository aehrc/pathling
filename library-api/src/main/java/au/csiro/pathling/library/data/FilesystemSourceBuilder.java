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

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import au.csiro.pathling.query.ImmutableDataSource;
import au.csiro.pathling.query.ImmutableDataSource.Builder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import scala.collection.JavaConverters;

public class FilesystemSourceBuilder extends AbstractSourceBuilder<FilesystemSourceBuilder> {

  @Nullable
  private String filesGlob;
  @Nonnull
  private Function<String, List<String>> filenameMapper;
  @Nonnull
  private DataFrameReader reader;
  @Nonnull
  private BiFunction<Dataset<Row>, String, Dataset<Row>> datasetTransformer;

  public FilesystemSourceBuilder(@Nonnull final PathlingContext pathlingContext) {
    super(pathlingContext);
    this.filenameMapper = SupportFunctions::basenameToResource;
    this.datasetTransformer = SupportFunctions::identityTransformer;
    this.reader = pathlingContext.getSpark().read().format("parquet");
  }

  @Nonnull
  public FilesystemSourceBuilder withFilesGlob(@Nonnull final String filesGlob) {
    this.filesGlob = filesGlob;
    return this;
  }

  @Nonnull
  public FilesystemSourceBuilder withFilenameMapper(
      @Nonnull final Function<String, List<String>> filenameMapper) {
    this.filenameMapper = filenameMapper;
    return this;
  }

  @Nonnull
  public FilesystemSourceBuilder withReader(@Nonnull final DataFrameReader reader) {
    this.reader = reader;
    return this;
  }

  @Nonnull
  public FilesystemSourceBuilder withFormat(@Nonnull final String format) {
    this.reader = pathlingContext.getSpark().read().format(format);
    return this;
  }

  @Nonnull
  public FilesystemSourceBuilder withDatasetTransformer(
      @Nonnull final BiFunction<Dataset<Row>, String, Dataset<Row>> datasetTransformer) {
    this.datasetTransformer = datasetTransformer;
    return this;
  }

  @Nonnull
  public FilesystemSourceBuilder withTextEncoder(@Nonnull final String mimeType) {
    return this
        .withFormat("text")
        .withDatasetTransformer(SupportFunctions.textEncodingTransformer(pathlingContext, mimeType));
  }
  
  @Nonnull
  @Override
  protected DataSource buildDataSource() {
    final org.apache.hadoop.conf.Configuration hadoopConfiguration = requireNonNull(
        pathlingContext.getSpark().sparkContext()
            .hadoopConfiguration());
    try {
      final FileSystem warehouse = FileSystem.get(
          new URI(requireNonNull(filesGlob, "Glob expression must not be null")),
          hadoopConfiguration);
      final Map<String, List<String>> filenamesByResourceTypes = Stream.of(
              warehouse.globStatus(new org.apache.hadoop.fs.Path(filesGlob)))
          .map(FileStatus::getPath)
          .map(org.apache.hadoop.fs.Path::toUri)
          .map(URI::normalize)
          .map(Object::toString)
          .flatMap(filepath -> filenameMapper.apply(filepath).stream()
              .map(resourceType -> Pair.of(resourceType, filepath)))
          .collect(Collectors.groupingBy(Pair::getKey,
              Collectors.mapping(Pair::getValue, Collectors.toList())));

      final Builder inMemoryDataSourceBuilder = ImmutableDataSource.builder();
      filenamesByResourceTypes.forEach((resourceType, filenames) ->
          inMemoryDataSourceBuilder.withResource(ResourceType.fromCode(resourceType),
              datasetTransformer.apply(reader.load(JavaConverters.asScalaBuffer(filenames).toSeq()),
                  resourceType)
          ));
      return inMemoryDataSourceBuilder.build();
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

}
