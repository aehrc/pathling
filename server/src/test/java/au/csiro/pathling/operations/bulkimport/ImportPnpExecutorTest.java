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

package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for ImportPnpExecutor, focusing on error message extraction from nested exceptions.
 *
 * @author John Grimes
 */
class ImportPnpExecutorTest {

  /**
   * Tests that when an exception is thrown with a null message but a nested cause with a message,
   * the root cause message is extracted. This simulates the Delta Lake error scenario where
   * exceptions are wrapped with intermediate exceptions that have null messages.
   */
  @Test
  void extractsRootCauseMessageWhenExceptionMessageIsNull() {
    // Given - an exception chain where the outermost exception has null message but the nested
    // cause contains the actual error message (simulating Delta/Spark exception wrapping).
    final String rootCauseMessage =
        "[DELTA_PATH_EXISTS] Cannot write to already existent path file:/test/Patient.parquet";
    final RuntimeException rootCause = new RuntimeException(rootCauseMessage);
    final RuntimeException wrappedException = new RuntimeException(null, rootCause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(wrappedException);

    // Then - the message should be the root cause message, not "null".
    assertThat(result)
        .contains("DELTA_PATH_EXISTS")
        .contains("Cannot write to already existent path");
  }

  /**
   * Tests that when an exception has a direct message, that message is used rather than traversing
   * the cause chain.
   */
  @Test
  void usesDirectMessageWhenAvailable() {
    // Given - an exception with a direct message.
    final String directMessage = "Direct error message";
    final RuntimeException exception = new RuntimeException(directMessage);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(exception);

    // Then - the message should be the direct message.
    assertThat(result).isEqualTo(directMessage);
  }

  /**
   * Tests that when an exception has multiple levels of nesting, the first non-null message in the
   * cause chain is used.
   */
  @Test
  void extractsFirstNonNullMessageFromDeepCauseChain() {
    // Given - a deeply nested exception chain where only the deepest cause has a message.
    final String deepCauseMessage = "Deep root cause error";
    final RuntimeException deepCause = new RuntimeException(deepCauseMessage);
    final RuntimeException middleCause = new RuntimeException(null, deepCause);
    final RuntimeException outerCause = new RuntimeException(null, middleCause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(outerCause);

    // Then - the message should be the deep cause message.
    assertThat(result).isEqualTo(deepCauseMessage);
  }

  /**
   * Tests that when an exception and all its causes have null messages, the exception class name is
   * used as a fallback.
   */
  @Test
  void usesExceptionClassNameWhenNoMessageAvailable() {
    // Given - an exception chain where all messages are null.
    final RuntimeException innerCause = new RuntimeException((String) null);
    final RuntimeException outerCause = new RuntimeException(null, innerCause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(outerCause);

    // Then - the result should be the exception class name.
    assertThat(result).isEqualTo("RuntimeException");
  }

  /**
   * Tests that blank messages are treated the same as null messages and the cause chain is
   * traversed.
   */
  @Test
  void treatsBlankMessageAsNull() {
    // Given - an exception with a blank message but a cause with a real message.
    final String causeMessage = "Actual error message";
    final RuntimeException cause = new RuntimeException(causeMessage);
    final RuntimeException outerException = new RuntimeException("   ", cause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(outerException);

    // Then - the result should be the cause message, not blank spaces.
    assertThat(result).isEqualTo(causeMessage);
  }

  /** Tests that empty string messages are treated the same as null. */
  @Test
  void treatsEmptyStringMessageAsNull() {
    // Given - an exception with an empty message but a cause with a real message.
    final String causeMessage = "Nested error";
    final RuntimeException cause = new RuntimeException(causeMessage);
    final RuntimeException outerException = new RuntimeException("", cause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(outerException);

    // Then - the result should be the cause message.
    assertThat(result).isEqualTo(causeMessage);
  }

  /** Tests that the first non-blank message is used even if deeper causes also have messages. */
  @Test
  void usesFirstNonBlankMessageInChain() {
    // Given - an exception chain with messages at multiple levels.
    final RuntimeException deepCause = new RuntimeException("Deep message");
    final RuntimeException middleCause = new RuntimeException("Middle message", deepCause);
    final RuntimeException outerCause = new RuntimeException(null, middleCause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(outerCause);

    // Then - the first non-blank message (middle) should be returned.
    assertThat(result).isEqualTo("Middle message");
  }
}
