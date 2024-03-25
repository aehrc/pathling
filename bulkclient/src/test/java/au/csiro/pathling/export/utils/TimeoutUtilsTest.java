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

package au.csiro.pathling.export.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TimeoutUtilsTest {

  @Test
  void testIsInfiniteTimeout() {
    assertTrue(TimeoutUtils.isInfinite(TimeoutUtils.TIMEOUT_INFINITE));
    assertTrue(TimeoutUtils.isInfinite(Duration.ZERO));
    assertTrue(TimeoutUtils.isInfinite(Duration.ZERO.minusNanos(1)));
    assertFalse(TimeoutUtils.isInfinite(TimeoutUtils.TIMEOUT_NOW));
  }

  @Test
  void testTimeoutAt() {
    assertEquals(Instant.MAX, TimeoutUtils.toTimeoutAt(TimeoutUtils.TIMEOUT_INFINITE));
    final Instant before = Instant.now();
    final Instant after = TimeoutUtils.toTimeoutAt(TimeoutUtils.TIMEOUT_NOW);
    assertTrue(after.isAfter(before));
    assertTrue(after.isBefore(Instant.now()));
  }

  @Test
  void testTimeoutAfter() {
    assertEquals(TimeoutUtils.TIMEOUT_INFINITE, TimeoutUtils.toTimeoutAfter(Instant.MAX));
    assertEquals(TimeoutUtils.TIMEOUT_NOW,
        TimeoutUtils.toTimeoutAfter(Instant.now().minusNanos(1)));
  }
}
