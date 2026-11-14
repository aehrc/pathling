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

package au.csiro.pathling.io;

import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Round-trips a recursive element nested far deeper than any corpus carries (FR-017, driver 3).
 *
 * <p>The depth of a recursive type is the data's to choose, and neither the specification examples
 * nor the Synthea corpus goes beyond a handful of levels. A transform whose expression grew with
 * the depth of the schema would pass over both and fail on a real questionnaire, which is what the
 * first review of the rescoped milestone found: the pruning each level applied was built from the
 * columns of the level below it, so the expression doubled per level and a depth of eight exhausted
 * the heap. Decision 71 removed the pruning, and this pins what the removal bought.
 */
class NestingDepthRoundTripTest {

  /** Deeper than any corpus in this repository, and deep enough to have failed before. */
  private static final int DEPTH = 12;

  @Test
  void returnsARecursiveElementNestedWellBeyondAnyCorpus(@TempDir @Nonnull final Path directory)
      throws IOException {
    final Path corpus = directory.resolve("questionnaires.ndjson");
    Files.write(corpus, List.of(questionnaire()));

    RoundTripHarness.unconditional().assertRoundTrip("Questionnaire", corpus);

    assertTrue(
        questionnaire().contains("\"linkId\":\"level-" + DEPTH + "\""), "the depth is built");
  }

  /** Returns a questionnaire whose item nests {@link #DEPTH} levels deep, each level answerable. */
  @Nonnull
  private static String questionnaire() {
    final StringBuilder document =
        new StringBuilder("{\"resourceType\":\"Questionnaire\",\"id\":\"1\",\"status\":\"draft\"");
    for (int level = 1; level <= DEPTH; level++) {
      document
          .append(",\"item\":[{\"linkId\":\"level-")
          .append(level)
          .append("\",\"text\":\"Question at level ")
          .append(level)
          .append("\",\"type\":\"group\"");
    }
    document.append("}]".repeat(DEPTH)).append('}');
    return document.toString();
  }
}
