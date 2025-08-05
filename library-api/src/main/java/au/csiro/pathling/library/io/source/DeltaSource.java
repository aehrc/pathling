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

package au.csiro.pathling.library.io.source;

import static au.csiro.pathling.library.io.FileSystemPersistence.getFileSystem;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.FileSystemPersistence;
import au.csiro.pathling.library.io.PersistenceError;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A class for making FHIR data in Delta tables on the filesystem available for query.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class DeltaSource extends AbstractSource {

  @Nonnull
  private final String path;

  /**
   * Constructs a DeltaSource with the specified PathlingContext and path.
   *
   * @param context the PathlingContext to use
   * @param path the path to the Delta table
   */
  public DeltaSource(@Nonnull final PathlingContext context, @Nonnull final String path) {
    super(context);
    this.path = path;
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final String resourceCode) {
    requireNonNull(resourceCode);
    return DeltaTable.forPath(context.getSpark(),
        FileSystemPersistence.getTableUrl(path, resourceCode)).df();
  }

  @Nonnull
  @Override
  public Set<String> getResourceTypes() {
    try {
      final Stream<FileStatus> files = Stream.of(
          getFileSystem(context.getSpark(), path).listStatus(new Path(path)));
      return files
          .map(FileStatus::getPath)
          .map(Path::getName)
          .map(fileName -> fileName.replace(".parquet", ""))
          .collect(Collectors.toSet());
    } catch (final IOException e) {
      throw new PersistenceError("Problem listing resources", e);
    }
  }

}
