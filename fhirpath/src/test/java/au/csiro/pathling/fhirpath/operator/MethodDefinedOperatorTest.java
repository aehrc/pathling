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

package au.csiro.pathling.fhirpath.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import org.junit.jupiter.api.Test;

/**
 * Tests for how {@link MethodDefinedOperator} reports errors raised by the operator method.
 *
 * @author John Grimes
 */
class MethodDefinedOperatorTest {

  private final EvaluationContext context = mock(EvaluationContext.class);

  /** An operator method that fails with an unexpected, non-user error. */
  @SuppressWarnings("unused")
  static Collection failingOperator(final Collection left, final Collection right) {
    throw new IllegalStateException("boom");
  }

  @Test
  void invalidUserInputFromOperatorIsNotWrapped() throws NoSuchMethodException {
    // A user error raised by the operator method, here a String index, must reach the caller as
    // an InvalidUserInputError with its own message, rather than a generic invocation error.
    final MethodDefinedOperator operator =
        MethodDefinedOperator.build(
            SubsettingOperations.class.getDeclaredMethod(
                "index", Collection.class, Collection.class));
    final BinaryOperatorInput input =
        new BinaryOperatorInput(
            context, StringCollection.fromValue("x"), StringCollection.fromValue("a"));

    final InvalidUserInputError error =
        assertThrows(InvalidUserInputError.class, () -> operator.invoke(input));
    assertEquals("Indexer operator requires an Integer index", error.getMessage());
  }

  @Test
  void otherErrorsFromOperatorAreWrapped() throws NoSuchMethodException {
    // An unexpected error is still reported as an invocation error, with the original as cause.
    final MethodDefinedOperator operator =
        MethodDefinedOperator.build(
            MethodDefinedOperatorTest.class.getDeclaredMethod(
                "failingOperator", Collection.class, Collection.class));
    final BinaryOperatorInput input =
        new BinaryOperatorInput(
            context, StringCollection.fromValue("x"), StringCollection.fromValue("y"));

    final MethodInvocationError error =
        assertThrows(MethodInvocationError.class, () -> operator.invoke(input));
    assertInstanceOf(IllegalStateException.class, error.getCause().getCause());
  }
}
