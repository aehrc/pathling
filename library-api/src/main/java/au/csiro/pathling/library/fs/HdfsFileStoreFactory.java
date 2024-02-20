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

package au.csiro.pathling.library.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import javax.annotation.Nonnull;
import au.csiro.pathling.export.fs.FileStore;
import au.csiro.pathling.export.fs.FileStoreFactory;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;

public class HdfsFileStoreFactory implements FileStoreFactory {

  @Nonnull
  private final Configuration configuration;

  public HdfsFileStoreFactory(@Nonnull final Configuration configuration) {
    // here we use scala.Option
    this.configuration = configuration;
  }

  @Nonnull
  public static HdfsFileStoreFactory ofSpark(@Nonnull final SparkContext sparkContext) {
    return new HdfsFileStoreFactory(sparkContext.hadoopConfiguration());
  }

  public HdfsFileStoreFactory() {
    this(new Configuration());
  }

  @Override
  public FileStore createFileStore(@Nonnull final String location) throws IOException {
    return new HdfsFileStore(FileSystem.get(URI.create(location), configuration));
  }

  @Slf4j
  static class HdfsFileStore implements FileStore {

    @Nonnull
    private final FileSystem fileSystem;

    HdfsFileStore(@Nonnull final FileSystem fileSystem) {
      this.fileSystem = fileSystem;
    }

    @Nonnull
    @Override
    public FileHandle get(@Nonnull final String location) {
      return new HdfsFileHandle(new Path(location));
    }

    @Override
    public void close() throws IOException {
      fileSystem.close();
    }

    @Value
    class HdfsFileHandle implements FileHandle {

      @Nonnull
      Path path;

      @Override
      public boolean exists() {
        try {
          return fileSystem.exists(path);
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public boolean mkdirs() {
        try {
          return fileSystem.mkdirs(path);
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Nonnull
      @Override
      public FileHandle child(@Nonnull final String childName) {
        return new HdfsFileHandle(new Path(path, childName));
      }

      @Nonnull
      @Override
      public String getLocation() {
        return path.toString();
      }

      @Nonnull
      @Override
      public URI toUri() {
        return path.toUri();
      }

      @Override
      public long writeAll(@Nonnull final InputStream is) throws IOException {
        try (final OutputStream os = fileSystem.create(path)) {
          return IOUtils.copyLarge(is, os);
        }
      }

      @Override
      @Nonnull
      public String toString() {
        return getLocation();
      }
    }
  }
}
