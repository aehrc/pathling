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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ServerInstanceId}.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class ServerInstanceIdTest {

  @Test
  void generatesUniqueIdOnStartup() {
    // Each new instance should generate a unique ID, simulating server restarts.
    final ServerInstanceId instance1 = new ServerInstanceId();
    final ServerInstanceId instance2 = new ServerInstanceId();

    assertNotEquals(instance1.getId(), instance2.getId());
  }

  @Test
  void idRemainsConstantForSameInstance() {
    // The same instance should return the same ID on repeated calls.
    final ServerInstanceId instance = new ServerInstanceId();
    final String firstCall = instance.getId();
    final String secondCall = instance.getId();

    assertEquals(firstCall, secondCall);
  }

  @Test
  void idHasExpectedFormat() {
    // ID should be an 8-character UUID prefix.
    final ServerInstanceId instance = new ServerInstanceId();
    final String id = instance.getId();

    assertNotNull(id);
    assertEquals(8, id.length());
  }
}
