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

package au.csiro.pathling.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RequestTag} covering field accessors, equality, and the custom toString
 * implementation that redacts sensitive header data.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class RequestTagTest {

  @Test
  void fieldAccessorsReturnConstructedValues() {
    // All fields should be accessible via the Lombok-generated getters.
    final Map<String, List<String>> headers = Map.of("Authorization", List.of("Bearer token"));
    final RequestTag tag =
        new RequestTag("http://example.com/Patient", headers, Optional.of("key1"), "opKey");

    assertEquals("http://example.com/Patient", tag.getRequestUrl());
    assertEquals(headers, tag.getVaryHeaders());
    assertEquals(Optional.of("key1"), tag.getCacheKey());
    assertEquals("opKey", tag.getOperationCacheKey());
  }

  @Test
  void toStringExcludesVaryHeaderValues() {
    // The toString method should not include the actual header values for security.
    final Map<String, List<String>> headers = Map.of("Authorization", List.of("Bearer secret"));
    final RequestTag tag =
        new RequestTag("http://example.com/Patient", headers, Optional.of("key1"), "opKey");

    final String result = tag.toString();
    assertTrue(result.contains("requestUrl='http://example.com/Patient'"));
    assertTrue(result.contains("cacheKey=Optional[key1]"));
    assertTrue(result.contains("operationCacheKey='opKey'"));
    // The actual header values should not appear in the string.
    assertFalse(result.contains("Bearer secret"));
    assertFalse(result.contains("Authorization"));
    // Instead, a hash should be present.
    assertTrue(result.contains("varyHeaders=List@"));
  }

  @Test
  void toStringWithEmptyCacheKey() {
    // A RequestTag with an empty cache key should show Optional.empty in toString.
    final RequestTag tag =
        new RequestTag("http://example.com", Map.of(), Optional.empty(), "opKey");

    final String result = tag.toString();
    assertTrue(result.contains("cacheKey=Optional.empty"));
  }

  @Test
  void equalTagsAreEqual() {
    // Two RequestTags with the same field values should be equal.
    final Map<String, List<String>> headers = Map.of("Accept", List.of("application/json"));
    final RequestTag tag1 =
        new RequestTag("http://example.com", headers, Optional.of("key"), "opKey");
    final RequestTag tag2 =
        new RequestTag("http://example.com", headers, Optional.of("key"), "opKey");

    assertEquals(tag1, tag2);
    assertEquals(tag1.hashCode(), tag2.hashCode());
  }

  @Test
  void differentTagsAreNotEqual() {
    // RequestTags with different field values should not be equal.
    final RequestTag tag1 =
        new RequestTag("http://example.com/A", Map.of(), Optional.empty(), "opKey");
    final RequestTag tag2 =
        new RequestTag("http://example.com/B", Map.of(), Optional.empty(), "opKey");

    assertNotEquals(tag1, tag2);
  }

  @Test
  void implementsJobTag() {
    // RequestTag should implement the Job.JobTag interface.
    final RequestTag tag = new RequestTag("http://example.com", Map.of(), Optional.empty(), "");
    assertTrue(tag instanceof Job.JobTag);
  }
}
