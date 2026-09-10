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

package au.csiro.pathling.terminology.store;

import jakarta.annotation.Nonnull;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Computes the SHA-1 and SHA-256 of everything read through a stream, so a source can be hashed
 * during the pass that already reads it rather than in a pass of its own. A reader that stops early
 * calls {@link #drain()} to pull the remaining bytes through the digests, so the hashes always
 * cover the whole file.
 *
 * @author John Grimes
 */
public class DigestingInputStream extends FilterInputStream {

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private static final int DRAIN_BUFFER_BYTES = 8192;

  @Nonnull private final MessageDigest sha1 = digest("SHA-1");

  @Nonnull private final MessageDigest sha256 = digest("SHA-256");

  /**
   * Wraps a stream so that every byte read through it is digested.
   *
   * @param in the stream to digest
   */
  public DigestingInputStream(@Nonnull final InputStream in) {
    super(in);
  }

  @Override
  public int read() throws IOException {
    final int value = in.read();
    if (value != -1) {
      sha1.update((byte) value);
      sha256.update((byte) value);
    }
    return value;
  }

  @Override
  public int read(@Nonnull final byte[] buffer, final int offset, final int length)
      throws IOException {
    final int read = in.read(buffer, offset, length);
    if (read > 0) {
      sha1.update(buffer, offset, read);
      sha256.update(buffer, offset, read);
    }
    return read;
  }

  /**
   * Skipped bytes are read rather than seeked past, so that they are digested like any other. A
   * digest of part of a file is worse than no digest at all.
   */
  @Override
  public long skip(final long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    final byte[] buffer = new byte[(int) Math.min(DRAIN_BUFFER_BYTES, n)];
    long skipped = 0;
    while (skipped < n) {
      final int read = read(buffer, 0, (int) Math.min(buffer.length, n - skipped));
      if (read == -1) {
        break;
      }
      skipped += read;
    }
    return skipped;
  }

  /**
   * Reads whatever remains of the stream so the digests cover the whole file, even when the reader
   * stopped at a structure that ends before the last byte.
   *
   * @throws IOException if the remainder cannot be read
   */
  public void drain() throws IOException {
    final byte[] buffer = new byte[DRAIN_BUFFER_BYTES];
    while (read(buffer, 0, buffer.length) != -1) {
      // The read itself updates the digests.
    }
  }

  /**
   * Returns the SHA-1 of everything read so far.
   *
   * @return the digest as lowercase hexadecimal
   */
  @Nonnull
  public String sha1Hex() {
    return hex(snapshot(sha1));
  }

  /**
   * Returns the SHA-256 of everything read so far.
   *
   * @return the digest as lowercase hexadecimal
   */
  @Nonnull
  public String sha256Hex() {
    return hex(snapshot(sha256));
  }

  /** Digests a copy, so the stream can go on being read after its hash has been taken. */
  @Nonnull
  private static byte[] snapshot(@Nonnull final MessageDigest digest) {
    try {
      return ((MessageDigest) digest.clone()).digest();
    } catch (final CloneNotSupportedException e) {
      throw new IllegalStateException(digest.getAlgorithm() + " digests cannot be copied", e);
    }
  }

  @Nonnull
  private static MessageDigest digest(@Nonnull final String algorithm) {
    try {
      return MessageDigest.getInstance(algorithm);
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException(algorithm + " is not available", e);
    }
  }

  @Nonnull
  private static String hex(@Nonnull final byte[] bytes) {
    final char[] chars = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      final int value = bytes[i] & 0xFF;
      chars[i * 2] = HEX[value >>> 4];
      chars[i * 2 + 1] = HEX[value & 0x0F];
    }
    return new String(chars);
  }
}
