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

import java.io.IOException;
import javax.annotation.Nonnull;

/**
 * Factory for creating {@link FileStore} instances.
 */
public interface FileStoreFactory {

  /**
   * Create a new {@link FileStore} instance for the given location. The location can be used to
   * determine the specific sub-type of file store to create, when necessary, e.g. based on the URI
   * scheme, but does not imply that the file is constrained to this location.
   *
   * @param location the location of the file store
   * @return a new {@link FileStore} instance
   * @throws IOException if an error occurs creating the file store
   */
 
  @Nonnull
  FileStore createFileStore(@Nonnull final String location) throws IOException;

  /**
   * Get the local file store factory.
   *
   * @return the local file store factory
   */
  @Nonnull
  static FileStoreFactory getLocal() {
    return LocalFileStore.FACTORY;
  }
}
