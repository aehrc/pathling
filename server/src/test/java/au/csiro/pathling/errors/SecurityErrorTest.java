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

package au.csiro.pathling.errors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SecurityError} covering both constructors and message propagation.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class SecurityErrorTest {

  @Test
  void messageConstructorPropagatesMessage() {
    // A SecurityError constructed with a message should return that message.
    final SecurityError error = new SecurityError("Access denied");
    assertEquals("Access denied", error.getMessage());
  }

  @Test
  void messageConstructorHasNullCause() {
    // A SecurityError constructed with only a message should have no cause.
    final SecurityError error = new SecurityError("Access denied");
    assertNull(error.getCause());
  }

  @Test
  void messageAndCauseConstructorPropagatesMessage() {
    // A SecurityError constructed with message and cause should return the message.
    final RuntimeException cause = new RuntimeException("underlying");
    final SecurityError error = new SecurityError("Access denied", cause);
    assertEquals("Access denied", error.getMessage());
  }

  @Test
  void messageAndCauseConstructorPropagatesCause() {
    // A SecurityError constructed with message and cause should return the cause.
    final RuntimeException cause = new RuntimeException("underlying");
    final SecurityError error = new SecurityError("Access denied", cause);
    assertSame(cause, error.getCause());
  }

  @Test
  void isRuntimeException() {
    // SecurityError should be a RuntimeException.
    final SecurityError error = new SecurityError("test");
    assertInstanceOf(RuntimeException.class, error);
  }
}
