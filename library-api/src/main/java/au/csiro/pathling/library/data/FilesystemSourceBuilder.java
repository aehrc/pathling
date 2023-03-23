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

/**
 * The {@link ReadableSource} builder that creates immutable data sources from files.
 */
public class FilesystemSourceBuilder extends AbstractSourceBuilder<FilesystemSourceBuilder> {

  @Nullable
  private String filesGlob;
  @Nonnull
  private Function<String, List<String>> filepathMapper;
  @Nonnull
  private DataFrameReader reader;
  @Nonnull
  private BiFunction<Dataset<Row>, String, Dataset<Row>> datasetTransformer;

  /**
   * Creates a new builder.
   */
  public FilesystemSourceBuilder(@Nonnull final PathlingContext pathlingContext) {
    super(pathlingContext);
    this.filepathMapper = SupportFunctions::basenameToResource;
    this.datasetTransformer = SupportFunctions::identityTransformer;
    this.reader = pathlingContext.getSpark().read().format("parquet");
  }

  /**
   * Sets the glob pattern to use to find files that should be included in the data source. The glob
   * can ba a path or URI in a hadoop compatible filesystem.
   *
   * @param filesGlob the files glob.
   * @return this builder.
   */
  @Nonnull
  public FilesystemSourceBuilder withFilesGlob(@Nonnull final String filesGlob) {
    this.filesGlob = filesGlob;
    return this;
  }

  /**
   * Sets the function that maps the path of a file to list of resource types it contains. The path
   * can be a filepath or a URI.
   *
   * @param filepathMapper the function that maps the path of a file to list of resource types it
   * contains.
   * @return this builder.
   */
  @Nonnull
  public FilesystemSourceBuilder withFilepathMapper(
      @Nonnull final Function<String, List<String>> filepathMapper) {
    this.filepathMapper = filepathMapper;
    return this;
  }

  /**
   * Sets the {@link DataFrameReader}  to use to read the files as dataframes.
   *
   * @param reader the data frame reader to use.
   * @return this builder.
   */
  @Nonnull
  public FilesystemSourceBuilder withReader(@Nonnull final DataFrameReader reader) {
    this.reader = reader;
    return this;
  }

  /**
   * Sets the format of the files to read. This is a shortcut for {@link
   * #withReader(DataFrameReader)}
   *
   * @param format the format of the files to read.
   * @return this builder.
   */
  @Nonnull
  public FilesystemSourceBuilder withFormat(@Nonnull final String format) {
    this.reader = pathlingContext.getSpark().read().format(format);
    return this;
  }

  /**
   * Sets the function that transforms the dataset of a resource type before it is added to the data
   * source. By default, no transformation is applied.
   *
   * @param datasetTransformer the transformer function.
   * @return this builder.
   */
  @Nonnull
  public FilesystemSourceBuilder withDatasetTransformer(
      @Nonnull final BiFunction<Dataset<Row>, String, Dataset<Row>> datasetTransformer) {
    this.datasetTransformer = datasetTransformer;
    return this;
  }


  /**
   * Sets the format of the files to read as text and sets the encoding of the text files.
   *
   * @param mimeType the mime type of the text files.
   * @return this builder.
   */
  @Nonnull
  public FilesystemSourceBuilder withTextEncoder(@Nonnull final String mimeType) {
    return this
        .withFormat("text")
        .withDatasetTransformer(
            SupportFunctions.textEncodingTransformer(pathlingContext, mimeType));
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
          .flatMap(filepath -> filepathMapper.apply(filepath).stream()
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
