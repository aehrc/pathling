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

package au.csiro.pathling.export.fs;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * An abstraction of a local or remote file store that can be used to read and write files and
 * create directories. The default implementation is {@link LocalFileStore} for local filesystem but
 * others can be implemented for remote filesystems such as HDFS, S3, etc.
 */
public interface FileStore extends Closeable {

  /**
   * Represents a pointer to a specific location in the file store.
   */
  interface FileHandle {

    /**
     * Check if the file exists.
     *
     * @return true if the file exists, false otherwise.
     */
    boolean exists();

    /**
     * Creates a directory (recursively) at the location of this file handle.
     *
     * @return true if the directory was created successfully, false otherwise.
     */
    boolean mkdirs();

    /**
     * Get a child file handle of this file handle.
     *
     * @param childName the name of the child file.
     * @return a file handle representing the child file.
     */
    @Nonnull
    FileHandle child(@Nonnull String childName);

    /**
     * Get the location of the file. The actual format of location is implementation specific. For
     * local filesystem it can be the path of the file, for others the URI.
     *
     * @return the location of the file.
     */
    @Nonnull
    String getLocation();

    /**
     * Get the URI of the file.
     */
    @Nonnull
    URI toUri();

    /**
     * Write the contents of the input stream to the file.
     *
     * @param inputStream the input stream to write.
     * @return the number of bytes written.
     * @throws IOException if an I/O error occurs.
     */
    long writeAll(@Nonnull InputStream inputStream) throws IOException;

    /**
     * Create a file handle for a local file.
     *
     * @param location the location of the file (the local filesystem path)
     * @return a file handle for the local file.
     */
    @Nonnull
    static FileHandle ofLocal(@Nonnull final String location) {
      return LocalFileStore.LocalFileHandle.of(location);
    }
  }

  /**
   * Get a file handle for the specified location. The actual format of location is implementation
   * specific. For local filesystem it can be the path of the file, for others the URI.
   *
   * @param location the location of the file.
   * @return a file handle for the file.
   */
  @Nonnull
  FileHandle get(@Nonnull String location);
}
