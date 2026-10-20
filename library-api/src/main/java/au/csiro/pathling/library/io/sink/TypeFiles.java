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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

/**
 * The files a sink writing a directory of files finds for a resource type in that directory: the
 * files the matching file source reads as the type. They are what a save mode is applied to, since
 * the directory a write is made into is renamed away once it is written. That renaming is here too,
 * since the names it gives the files are what keep one write's files from replacing another's.
 *
 * @author Piotr Szul
 */
@Slf4j
final class TypeFiles {

  /**
   * The name Spark gives a file it writes: the partition id and the id of the write, then an
   * optional file counter and the extensions. Databricks puts a transaction id, {@code
   * tid-<number>}, between the partition id and the id of the write.
   */
  @Nonnull
  private static final Pattern SPARK_PART_FILE =
      Pattern.compile(
          "^part-(\\d+)-(?:tid--?\\d+-)?([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})"
              + "(?:[-.].*)?$");

  private TypeFiles() {}

  /**
   * Lists the files in a directory that the sink gives the type: those with the extension whose
   * base name is the type's file name, alone or followed by a period and a qualifier without one,
   * such as a partition id. The table a write is made into, {@code <fileName>.<extension>}, is
   * among them where it exists.
   *
   * <p>The file name is matched as it is, not by the pattern the file source reads a type's files
   * with, which admits only letters. A file name the sink is given that is not only letters is
   * therefore not read as the type, but the save modes still find the files written under it.
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
          .filter(file -> isNamedFor(FilenameUtils.getBaseName(file.getName()), fileName))
          .toList();
    } catch (final IOException e) {
      throw new PersistenceError(
          "Problem listing the files of " + fileName + " in " + directory, e);
    }
  }

  /**
   * Returns whether a base name is a file name, alone or followed by a period and a qualifier that
   * holds no period.
   */
  private static boolean isNamedFor(
      @Nonnull final String baseName, @Nonnull final String fileName) {
    if (baseName.equals(fileName)) {
      return true;
    }
    final String prefix = fileName + ".";
    if (!baseName.startsWith(prefix)) {
      return false;
    }
    final String qualifier = baseName.substring(prefix.length());
    return !qualifier.isEmpty() && qualifier.indexOf('.') < 0;
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

  /**
   * Replaces the directory a write was made into with its files, renamed to follow the naming
   * convention in {@link FileSource#resourceNameWithQualifierMapper(String)} and keeping the id
   * Spark gave the write.
   *
   * <p>Each file is named {@code <name>.<partId>-<writeId>.<extension>}, for example {@code
   * Patient.00000-7a60a041-0e7d-4e24-b23d-82405a665677.parquet}, where the table is {@code
   * <name>.<extension>}. Spark gives every write an id of its own, so the files of a write never
   * take the names of an earlier write's files in the same directory, and a write can be added
   * beside them.
   *
   * <p>A file in the table that Spark did not name, which a table assembled by hand can hold, is
   * left there, and so is the table: it is still read as the type, so the rows it holds are kept.
   *
   * @param spark the Spark session
   * @param table the directory the write was made into, {@code <name>.<extension>}
   * @param partExtension the extension of the files Spark wrote in the table
   * @return the paths of the renamed files
   * @throws PersistenceError if the table holds no file named as Spark names them, or there is a
   *     problem renaming one, in which case the table is left in place
   */
  @Nonnull
  static List<String> renameKeepingWriteId(
      @Nonnull final SparkSession spark,
      @Nonnull final String table,
      @Nonnull final String partExtension) {
    return renameKeepingWriteId(
        FileSystemPersistence.getFileSystem(spark, table), table, partExtension);
  }

  /**
   * Replaces the directory a write was made into with its files, renamed as {@link
   * #renameKeepingWriteId(SparkSession, String, String)} describes.
   *
   * @param fileSystem the file system holding the table
   * @param table the directory the write was made into, {@code <name>.<extension>}
   * @param partExtension the extension of the files Spark wrote in the table
   * @return the paths of the renamed files
   * @throws PersistenceError if the table is not named {@code <name>.<extension>} or holds no file
   *     with the extension named as Spark names them, or there is a problem renaming one, in which
   *     case the table is left in place
   */
  @Nonnull
  static List<String> renameKeepingWriteId(
      @Nonnull final FileSystem fileSystem,
      @Nonnull final String table,
      @Nonnull final String partExtension) {
    final Path tablePath = new Path(table);
    final String[] nameAndExtension = tablePath.getName().split("\\.");
    if (nameAndExtension.length != 2) {
      throw new PersistenceError(
          "Expected the table %s to be named <name>.<extension>".formatted(table), null);
    }
    try {
      final List<Path> files =
          Arrays.stream(fileSystem.listStatus(tablePath))
              .map(FileStatus::getPath)
              .filter(file -> FilenameUtils.isExtension(file.getName(), partExtension))
              .toList();
      final List<Path> parts =
          files.stream().filter(file -> SPARK_PART_FILE.matcher(file.getName()).matches()).toList();
      if (parts.isEmpty()) {
        throw new PersistenceError(
            "No partition files were found in %s; expected files named part-<partitionId>-<writeId>"
                .formatted(table),
            null);
      }
      final List<String> renamed = new ArrayList<>();
      for (final Path part : parts) {
        final Path target =
            new Path(
                tablePath.getParent(),
                "%s.%s.%s"
                    .formatted(
                        nameAndExtension[0], partitionAndWriteId(part), nameAndExtension[1]));
        log.info("Renaming result to: {}", target);
        // A rename that fails must not be followed by the removal of the table, which still holds
        // the file.
        if (!fileSystem.rename(part, target)) {
          throw new PersistenceError(
              "Could not rename the partition file %s to %s; the table %s has been left in place"
                  .formatted(part, target, table),
              null);
        }
        renamed.add(target.toString());
      }
      if (parts.size() == files.size()) {
        log.info("Cleaning up: {}", table);
        fileSystem.delete(tablePath, true);
      } else {
        log.warn("Leaving {} in place, as it holds files that Spark did not name", table);
      }
      return renamed;
    } catch (final IOException e) {
      throw new PersistenceError("Problem renaming the partition files of " + table, e);
    }
  }

  /**
   * Returns the partition id and the write id of a file Spark wrote, for example {@code
   * 00000-<writeId>} from {@code part-00000-<writeId>-c000.snappy.parquet}.
   */
  @Nonnull
  private static String partitionAndWriteId(@Nonnull final Path part) {
    final Matcher matcher = SPARK_PART_FILE.matcher(part.getName());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Not a file Spark named: " + part);
    }
    return matcher.group(1) + "-" + matcher.group(2);
  }
}
