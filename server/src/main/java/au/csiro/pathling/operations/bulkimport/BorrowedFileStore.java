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

package au.csiro.pathling.operations.bulkimport;

import au.csiro.filestore.FileStore;
import jakarta.annotation.Nonnull;

/**
 * A {@link FileStore} that delegates all file access to another store, but does not release the
 * underlying resources when it is closed.
 *
 * <p>The bulk export client manages its file store in a try-with-resources block, and the Hadoop
 * store it is given closes the Hadoop file system when it is closed. That file system comes from a
 * JVM-wide cache shared with the rest of the server, so allowing the export to close it leaves the
 * server unable to reach the warehouse until it is restarted. Wrapping the store keeps the file
 * system open for the staging directory to be listed after the download, and for all later work.
 *
 * @author John Grimes
 */
class BorrowedFileStore implements FileStore {

  @Nonnull private final FileStore delegate;

  /**
   * Creates a store that delegates to another store without taking ownership of it.
   *
   * @param delegate the store to delegate file access to
   */
  BorrowedFileStore(@Nonnull final FileStore delegate) {
    this.delegate = delegate;
  }

  @Nonnull
  @Override
  public FileHandle get(@Nonnull final String location) {
    return delegate.get(location);
  }

  @Override
  public void close() {
    // The file system is borrowed from the server, which remains responsible for its lifecycle.
  }
}
