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

public interface FileStore extends Closeable {

  interface FileHandle {

    boolean exists();

    boolean mkdirs();

    @Nonnull
    FileHandle child(@Nonnull String childName);

    @Nonnull
    String getLocation();

    @Nonnull
    URI toUri();

    long writeAll(@Nonnull InputStream inputStream) throws IOException;

    @Nonnull
    static FileHandle ofLocal(@Nonnull final String location) {
      return LocalFileStore.LocalFileHandle.of(location);
    }
  }

  @Nonnull
  FileHandle get(@Nonnull String location);
}
