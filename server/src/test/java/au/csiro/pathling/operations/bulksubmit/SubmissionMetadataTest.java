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

package au.csiro.pathling.operations.bulksubmit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SubmissionMetadata} covering construction with and without values.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class SubmissionMetadataTest {

  @Test
  void constructsWithValues() {
    // A SubmissionMetadata with values should return those values.
    final Map<String, String> values = Map.of("key1", "value1", "key2", "value2");
    final SubmissionMetadata metadata = new SubmissionMetadata(values);
    assertEquals(values, metadata.values());
  }

  @Test
  void constructsWithNullValues() {
    // A SubmissionMetadata with null values should return null.
    final SubmissionMetadata metadata = new SubmissionMetadata(null);
    assertNull(metadata.values());
  }

  @Test
  void equalMetadataAreEqual() {
    // Two SubmissionMetadata instances with the same values should be equal.
    final Map<String, String> values = Map.of("key", "value");
    final SubmissionMetadata metadata1 = new SubmissionMetadata(values);
    final SubmissionMetadata metadata2 = new SubmissionMetadata(values);
    assertEquals(metadata1, metadata2);
    assertEquals(metadata1.hashCode(), metadata2.hashCode());
  }

  @Test
  void differentMetadataAreNotEqual() {
    // SubmissionMetadata instances with different values should not be equal.
    final SubmissionMetadata metadata1 = new SubmissionMetadata(Map.of("a", "1"));
    final SubmissionMetadata metadata2 = new SubmissionMetadata(Map.of("b", "2"));
    assertNotEquals(metadata1, metadata2);
  }
}
