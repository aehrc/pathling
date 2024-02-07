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

package au.csiro.pathling.export;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RetryValueTest {
  
  @Test
  void testParsesSecondValue() {
    assertEquals(Optional.of(RetryValue.after(Duration.ofSeconds(5))), RetryValue.parse("5"));
  }
  
  @Test
  void testEmptyForUnparsableStringString() {
    assertEquals(Optional.empty(), RetryValue.parse("5xxx3"));
  }

  @Test
  void testParsesHttpDateValue() {
    assertEquals(Optional.of(RetryValue.at(Instant.parse("2019-07-22T23:59:59Z"))),
        RetryValue.parse("Mon, 22 Jul 2019 23:59:59 GMT"));
  }
  
  @Test 
  void computesUntilCorrectlyForAfter() {
    final RetryValue value = RetryValue.after(Duration.ofSeconds(3));
    assertEquals(Duration.ofSeconds(3), value.until(Instant.now()));
  }
  
  @Test
  void computesUntilCorrectlyForAt() {
    final Instant timeMoment = Instant.parse("2019-07-22T23:59:59Z");
    final RetryValue value = RetryValue.at(timeMoment.plusSeconds(10));
    assertEquals(Duration.ofSeconds(10), value.until(timeMoment));
  }
  
}
