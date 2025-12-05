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
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 * An input stream wrapper that implements the Hadoop Seekable and PositionedReadable interfaces for
 * HTTP connections.
 * <p>
 * This implementation tracks the current position and supports forward seeking by reading and
 * discarding bytes. Backward seeking is not supported as HTTP streams are inherently sequential.
 * Positioned reads are also not supported. This is sufficient for Spark to read NDJSON files.
 *
 * @author John Grimes
 */
public class HttpInputStream extends InputStream implements Seekable, PositionedReadable {

  @Nonnull
  private final HttpURLConnection connection;

  @Nonnull
  private final InputStream inputStream;

  private long position = 0;

  /**
   * Creates a new HttpInputStream wrapping the given HTTP connection.
   *
   * @param connection the HTTP connection to read from
   * @throws IOException if an error occurs opening the input stream
   */
  public HttpInputStream(@Nonnull final HttpURLConnection connection) throws IOException {
    this.connection = connection;
    this.inputStream = connection.getInputStream();
  }

  @Override
  public int read() throws IOException {
    final int b = inputStream.read();
    if (b >= 0) {
      position++;
    }
    return b;
  }

  @Override
  public int read(@Nonnull final byte[] b, final int off, final int len) throws IOException {
    final int bytesRead = inputStream.read(b, off, len);
    if (bytesRead > 0) {
      position += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public void close() throws IOException {
    try {
      inputStream.close();
    } finally {
      connection.disconnect();
    }
  }

  @Override
  public void seek(final long pos) throws IOException {
    if (pos == position) {
      // Already at the requested position.
      return;
    }
    if (pos > position) {
      // Seeking forward - skip bytes.
      final long bytesToSkip = pos - position;
      long skipped = 0;
      while (skipped < bytesToSkip) {
        final long n = inputStream.skip(bytesToSkip - skipped);
        if (n <= 0) {
          // Read and discard if skip doesn't work.
          if (inputStream.read() < 0) {
            throw new IOException("Unexpected end of stream while seeking");
          }
          skipped++;
        } else {
          skipped += n;
        }
      }
      position = pos;
      return;
    }
    // Seeking backward is not supported for HTTP streams.
    throw new IOException("HTTP streams do not support seeking backward (from " + position
        + " to " + pos + ")");
  }

  @Override
  public long getPos() throws IOException {
    return position;
  }

  @Override
  public boolean seekToNewSource(final long targetPos) throws IOException {
    return false;
  }

  @Override
  public int read(final long position, @Nonnull final byte[] buffer, final int offset,
      final int length)
      throws IOException {
    throw new UnsupportedOperationException("HTTP streams do not support positioned reads");
  }

  @Override
  public void readFully(final long position, @Nonnull final byte[] buffer, final int offset,
      final int length)
      throws IOException {
    throw new UnsupportedOperationException("HTTP streams do not support positioned reads");
  }

  @Override
  public void readFully(final long position, @Nonnull final byte[] buffer) throws IOException {
    throw new UnsupportedOperationException("HTTP streams do not support positioned reads");
  }

}
