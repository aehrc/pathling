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
package au.csiro.pathling.library.io.sink;

import au.csiro.pathling.library.io.FileSystemPersistence;
import au.csiro.pathling.library.io.PersistenceError;
import au.csiro.pathling.library.io.source.FileSource;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

/**
 * The files a sink writing a directory of files finds for a resource type in that directory: the
 * files the matching file source reads as the type. They are what a save mode is applied to, since
 * the directory a write is made into is renamed away once it is written.
 *
 * @author Piotr Szul
 */
final class TypeFiles {

  private TypeFiles() {}

  /**
   * Lists the files in a directory that the file source reads as the type: those with the extension
   * whose base name, less any qualifier such as a partition id, is the type's file name. The table
   * a write is made into, {@code <fileName>.<extension>}, is among them where it exists.
   *
   * @param spark the Spark session
   * @param directory the directory the sink writes
   * @param fileName the file name the sink gives the type
   * @param extension the extension of the files the sink writes
   * @return the paths of the files, empty where the directory does not exist
   * @throws PersistenceError if the directory cannot be listed
   */
  @Nonnull
  static List<Path> list(
      @Nonnull final SparkSession spark,
      @Nonnull final String directory,
      @Nonnull final String fileName,
      @Nonnull final String extension) {
    final Path directoryPath = new Path(directory);
    final FileSystem fileSystem = FileSystemPersistence.getFileSystem(spark, directory);
    try {
      if (!fileSystem.exists(directoryPath)) {
        return List.of();
      }
      return Arrays.stream(fileSystem.listStatus(directoryPath))
          .map(FileStatus::getPath)
          .filter(file -> FilenameUtils.isExtension(file.getName(), extension))
          .filter(
              file ->
                  FileSource.resourceNameWithQualifierMapper(
                          FilenameUtils.getBaseName(file.getName()))
                      .contains(fileName))
          .toList();
    } catch (final IOException e) {
      throw new PersistenceError(
          "Problem listing the files of " + fileName + " in " + directory, e);
    }
  }

  /**
   * Refuses a write made with the error-if-exists save mode where the type already has files.
   *
   * @param resourceType the resource type being written
   * @param existing the files the type already has
   * @param directory the directory the sink writes
   * @throws PersistenceError if the type has files
   */
  static void refuseIfAny(
      @Nonnull final String resourceType,
      @Nonnull final Collection<Path> existing,
      @Nonnull final String directory) {
    if (!existing.isEmpty()) {
      throw new PersistenceError(
          ("The %s data already exists in %s, and the save mode is error-if-exists: use append "
                  + "or merge to add to it, overwrite to replace it, or ignore to leave it")
              .formatted(resourceType, directory),
          null);
    }
  }

  /**
   * Deletes the files a type had before an overwrite, once the files that replace them are in
   * place. The table the overwrite was written into is left out, since the write replaced it and
   * the rename that followed removed it.
   *
   * @param spark the Spark session
   * @param directory the directory the sink writes
   * @param replaced the files the type had before the overwrite
   * @param table the table the overwrite was written into
   * @throws PersistenceError if a file cannot be deleted
   */
  static void deleteReplaced(
      @Nonnull final SparkSession spark,
      @Nonnull final String directory,
      @Nonnull final Collection<Path> replaced,
      @Nonnull final String table) {
    final FileSystem fileSystem = FileSystemPersistence.getFileSystem(spark, directory);
    final Path tablePath = new Path(table);
    for (final Path file : replaced) {
      if (file.getName().equals(tablePath.getName())) {
        continue;
      }
      try {
        if (fileSystem.exists(file) && !fileSystem.delete(file, true)) {
          throw new PersistenceError("Could not delete the replaced file " + file, null);
        }
      } catch (final IOException e) {
        throw new PersistenceError("Problem deleting the replaced file " + file, e);
      }
    }
  }
}
