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

package au.csiro.pathling.sof.benchmark;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class DataLocatorTest {

  @Test
  void resolvesIdentityPath() {
    final Path dataDir = DataLocator.locate(TestResources.dataRoot(), "synthea-clinical", "1", "s");

    assertTrue(Files.isDirectory(dataDir));
    assertTrue(Files.isRegularFile(dataDir.resolve("Condition.ndjson")));
    assertTrue(dataDir.endsWith(Path.of("synthea-clinical", "1", "s")));
  }

  @Test
  void missingDirectoryReportsIdentity() {
    final IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> DataLocator.locate(TestResources.dataRoot(), "synthea-clinical", "1", "xl"));

    assertTrue(error.getMessage().contains("synthea-clinical"));
    assertTrue(error.getMessage().contains("version=1"));
    assertTrue(error.getMessage().contains("size=xl"));
  }
}
