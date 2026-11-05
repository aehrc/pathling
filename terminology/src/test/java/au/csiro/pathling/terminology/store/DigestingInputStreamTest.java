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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the digesting stream reproduces the known digests of a short input, and that the
 * digests cover the whole stream whether its bytes were read, skipped or drained.
 *
 * @author John Grimes
 */
class DigestingInputStreamTest {

  /** The published SHA-1 of the three bytes {@code abc}. */
  private static final String ABC_SHA1 = "a9993e364706816aba3e25717850c26c9cd0d89d";

  /** The published SHA-256 of the three bytes {@code abc}. */
  private static final String ABC_SHA256 =
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

  /** The published SHA-1 of the empty input. */
  private static final String EMPTY_SHA1 = "da39a3ee5e6b4b0d3255bfef95601890afd80709";

  /** The published SHA-256 of the empty input. */
  private static final String EMPTY_SHA256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  @Test
  void digestsMatchTheKnownAnswersForABC() throws IOException {
    try (DigestingInputStream in = digesting("abc")) {
      assertEquals(3, in.read(new byte[8], 0, 8));
      assertEquals(ABC_SHA1, in.sha1Hex());
      assertEquals(ABC_SHA256, in.sha256Hex());
    }
  }

  @Test
  void digestsMatchTheKnownAnswersWhenReadOneByteAtATime() throws IOException {
    try (DigestingInputStream in = digesting("abc")) {
      while (in.read() != -1) {
        // Each single-byte read updates the digests.
      }
      assertEquals(ABC_SHA1, in.sha1Hex());
      assertEquals(ABC_SHA256, in.sha256Hex());
    }
  }

  @Test
  void drainCoversBytesTheReaderLeftUnread() throws IOException {
    try (DigestingInputStream in = digesting("abc")) {
      assertEquals('a', in.read());
      in.drain();
      // The reader stopped after one byte, yet the digests describe the whole input.
      assertEquals(ABC_SHA1, in.sha1Hex());
      assertEquals(ABC_SHA256, in.sha256Hex());
    }
  }

  @Test
  void skipIsIncludedInTheDigest() throws IOException {
    try (DigestingInputStream in = digesting("abc")) {
      assertEquals(2, in.skip(2));
      assertEquals('c', in.read());
      // Skipped bytes were read through the digests rather than seeked past.
      assertEquals(ABC_SHA1, in.sha1Hex());
      assertEquals(ABC_SHA256, in.sha256Hex());
    }
  }

  @Test
  void skipPastTheEndDigestsOnlyWhatExists() throws IOException {
    try (DigestingInputStream in = digesting("abc")) {
      assertEquals(3, in.skip(100));
      assertEquals(ABC_SHA256, in.sha256Hex());
    }
  }

  @Test
  void emptyStreamDigests() throws IOException {
    try (DigestingInputStream in = digesting("")) {
      in.drain();
      assertEquals(EMPTY_SHA1, in.sha1Hex());
      assertEquals(EMPTY_SHA256, in.sha256Hex());
    }
  }

  @Test
  void digestsCanBeReadRepeatedlyWhileReadingContinues() throws IOException {
    try (DigestingInputStream in = digesting("abc")) {
      assertEquals('a', in.read());
      final String afterFirstByte = in.sha256Hex();
      assertEquals(afterFirstByte, in.sha256Hex(), "taking a digest does not consume it");
      in.drain();
      assertEquals(ABC_SHA256, in.sha256Hex());
    }
  }

  private static DigestingInputStream digesting(final String content) {
    return new DigestingInputStream(
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
  }
}
