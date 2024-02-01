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

package au.csiro.pathling.export;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Value
@Slf4j
class HdfsFileStore implements FileStore {

  @Nonnull
  URI rootPath;

  @Nonnull
  FileSystem fileSystem;

  @Nonnull
  @Override
  public URI writeTo(@Nonnull final String fileName, @Nonnull final InputStream is)
      throws IOException {
    final Path filePath = new Path(new Path(rootPath), fileName);
    try (final OutputStream os = fileSystem.create(filePath)) {
      IOUtils.copy(is, os);
    }
    return filePath.toUri();
  }

  @Override
  public void commit() throws IOException {
    final Path successPath = new Path(new Path(rootPath), "_SUCCESS");
    fileSystem.create(successPath).close();
    log.debug("Committing: {} ({})", rootPath, successPath);
  }

  public static HdfsFileStore of(@Nonnull final String rootPath) throws IOException {
    final Configuration conf = new Configuration();
    final URI fsURI = URI.create(rootPath);
    final FileSystem fs = FileSystem.get(conf);
    return new HdfsFileStore(fsURI, fs);
  }

}
