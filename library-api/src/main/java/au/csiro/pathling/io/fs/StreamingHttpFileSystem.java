/*
 * Copyright 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.io.fs;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A read-only Hadoop FileSystem implementation that supports streaming reads from HTTP URLs.
 * <p>
 * This implementation provides a working {@link #listStatus(Path)} method that returns the file
 * status for individual files, allowing Spark's DataFrameReader to read from HTTP URLs. Unlike
 * Hadoop's built-in HttpFileSystem, which throws UnsupportedOperationException for listStatus, this
 * implementation treats each path as an individual file.
 * <p>
 * This filesystem is designed for reading NDJSON files from bulk data export manifests, where the
 * exact file URLs are known in advance.
 *
 * @author John Grimes
 */
public class StreamingHttpFileSystem extends FileSystem {

  private static final int DEFAULT_BLOCK_SIZE = 4096;
  private static final int HTTP_OK = 200;
  private static final int HTTP_NOT_FOUND = 404;
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 60_000;

  @Nullable
  private URI uri;

  @Nonnull
  private Path workingDirectory = new Path("/");

  @Override
  public void initialize(@Nonnull final URI name, @Nonnull final Configuration conf)
      throws IOException {
    super.initialize(name, conf);
    this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());
    setConf(conf);
  }

  @Override
  @Nonnull
  public String getScheme() {
    return "http";
  }

  @Override
  @Nonnull
  public URI getUri() {
    if (uri == null) {
      throw new IllegalStateException("FileSystem not initialized");
    }
    return uri;
  }

  @Override
  @Nonnull
  public FSDataInputStream open(@Nonnull final Path path, final int bufferSize) throws IOException {
    final URL url = pathToUrl(path);
    final HttpURLConnection connection = openConnection(url);
    connection.setRequestMethod("GET");

    final int responseCode = connection.getResponseCode();
    if (responseCode == HTTP_NOT_FOUND) {
      connection.disconnect();
      throw new FileNotFoundException("File not found: " + path);
    }
    if (responseCode != HTTP_OK) {
      connection.disconnect();
      throw new IOException("HTTP request failed with status " + responseCode + ": " + path);
    }

    return new FSDataInputStream(new HttpInputStream(connection));
  }

  @Override
  @Nonnull
  public FileStatus getFileStatus(@Nonnull final Path path) throws IOException {
    final URL url = pathToUrl(path);
    final HttpURLConnection connection = openConnection(url);
    connection.setRequestMethod("HEAD");

    try {
      final int responseCode = connection.getResponseCode();
      if (responseCode == HTTP_NOT_FOUND) {
        throw new FileNotFoundException("File not found: " + path);
      }
      if (responseCode != HTTP_OK) {
        throw new IOException("HTTP request failed with status " + responseCode + ": " + path);
      }

      final long contentLength = connection.getContentLengthLong();
      final long lastModified = connection.getLastModified();
      final long modificationTime = lastModified > 0
                                    ? lastModified
                                    : System.currentTimeMillis();

      return new FileStatus(
          contentLength >= 0
          ? contentLength
          : 0,
          false,
          1,
          DEFAULT_BLOCK_SIZE,
          modificationTime,
          makeQualified(path)
      );
    } finally {
      connection.disconnect();
    }
  }

  @Override
  @Nonnull
  public FileStatus[] listStatus(@Nonnull final Path path) throws IOException {
    // Treat the path as a single file and return its status.
    // This allows Spark to read individual HTTP URLs without directory listing support.
    return new FileStatus[]{getFileStatus(path)};
  }

  @Override
  public boolean exists(@Nonnull final Path path) throws IOException {
    final URL url = pathToUrl(path);
    final HttpURLConnection connection = openConnection(url);
    connection.setRequestMethod("HEAD");

    try {
      final int responseCode = connection.getResponseCode();
      return responseCode == HTTP_OK;
    } finally {
      connection.disconnect();
    }
  }

  @Override
  @Nonnull
  public Path getWorkingDirectory() {
    return workingDirectory;
  }

  @Override
  public void setWorkingDirectory(@Nonnull final Path newDir) {
    this.workingDirectory = newDir;
  }

  // Unsupported write operations.

  @Override
  @Nonnull
  public FSDataOutputStream create(@Nonnull final Path path,
      @Nonnull final FsPermission permission, final boolean overwrite, final int bufferSize,
      final short replication, final long blockSize, @Nullable final Progressable progress)
      throws IOException {
    throw new UnsupportedOperationException("HTTP filesystem is read-only");
  }

  @Override
  @Nonnull
  public FSDataOutputStream append(@Nonnull final Path path, final int bufferSize,
      @Nullable final Progressable progress) throws IOException {
    throw new UnsupportedOperationException("HTTP filesystem is read-only");
  }

  @Override
  public boolean rename(@Nonnull final Path src, @Nonnull final Path dst) throws IOException {
    throw new UnsupportedOperationException("HTTP filesystem is read-only");
  }

  @Override
  public boolean delete(@Nonnull final Path path, final boolean recursive) throws IOException {
    throw new UnsupportedOperationException("HTTP filesystem is read-only");
  }

  @Override
  public boolean mkdirs(@Nonnull final Path path, @Nonnull final FsPermission permission)
      throws IOException {
    throw new UnsupportedOperationException("HTTP filesystem is read-only");
  }

  @Nonnull
  private URL pathToUrl(@Nonnull final Path path) throws IOException {
    return path.toUri().toURL();
  }

  @Nonnull
  private HttpURLConnection openConnection(@Nonnull final URL url) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
    connection.setReadTimeout(READ_TIMEOUT_MS);
    connection.setInstanceFollowRedirects(true);
    return connection;
  }

}
