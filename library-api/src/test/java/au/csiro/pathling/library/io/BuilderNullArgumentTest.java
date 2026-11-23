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

package au.csiro.pathling.library.io;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that every argument of the methods on {@link DataSourceBuilder} and {@link DataSinkBuilder}
 * is declared non-null, and that a null argument is rejected at run time. The run-time check is
 * needed as well as the annotation because these methods are also called from Python and R, where
 * the annotation is not checked.
 *
 * <p>The methods are found by reflection, so that a method added to either builder is covered
 * without changing this test.
 *
 * @author John Grimes
 */
class BuilderNullArgumentTest {

  /** The builder classes whose methods are tested. */
  @Nonnull
  private static Stream<Class<?>> builderClasses() {
    return Stream.of(DataSourceBuilder.class, DataSinkBuilder.class);
  }

  /**
   * Supplies one case for each reference-typed parameter of each public method declared by a
   * builder, other than the methods that override those of {@link Object}.
   */
  @Nonnull
  static Stream<Arguments> referenceParameters() {
    return builderClasses()
        .flatMap(builderClass -> Arrays.stream(builderClass.getDeclaredMethods()))
        .filter(method -> Modifier.isPublic(method.getModifiers()))
        .filter(method -> !Modifier.isStatic(method.getModifiers()))
        .filter(method -> !method.isSynthetic())
        .filter(method -> !overridesObjectMethod(method))
        .flatMap(
            method ->
                IntStream.range(0, method.getParameterCount())
                    .filter(index -> !method.getParameterTypes()[index].isPrimitive())
                    .mapToObj(index -> Arguments.of(describe(method, index), method, index)));
  }

  /** Every reference-typed parameter of a builder method is annotated as non-null. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("referenceParameters")
  void parameterIsDeclaredNonNull(
      @Nonnull final String description, @Nonnull final Method method, final int index) {
    // Arrange.
    final Parameter parameter = method.getParameters()[index];

    // Assert.
    assertTrue(
        parameter.isAnnotationPresent(Nonnull.class), description + " is not annotated @Nonnull");
    assertFalse(
        parameter.isAnnotationPresent(Nullable.class), description + " is annotated @Nullable");
  }

  /**
   * A null passed in any reference-typed parameter of a builder method is rejected with an {@link
   * IllegalArgumentException}, with every other argument supplied.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("referenceParameters")
  void nullArgumentIsRejected(
      @Nonnull final String description, @Nonnull final Method method, final int index) {
    // Arrange.
    final Object builder = newBuilder(method.getDeclaringClass());
    final Object[] arguments = nonNullArguments(method);
    arguments[index] = null;

    // Act.
    final InvocationTargetException thrown =
        assertThrows(InvocationTargetException.class, () -> method.invoke(builder, arguments));

    // Assert.
    assertInstanceOf(IllegalArgumentException.class, thrown.getCause(), description);
  }

  /** Creates a builder of the given class over mocked collaborators. */
  @Nonnull
  private static Object newBuilder(@Nonnull final Class<?> builderClass) {
    final PathlingContext context = mock(PathlingContext.class);
    if (builderClass == DataSourceBuilder.class) {
      return new DataSourceBuilder(context);
    }
    return new DataSinkBuilder(context, mock(DataSource.class));
  }

  /**
   * Builds a non-null argument for each parameter of a method: a string for a string parameter and
   * a mock for anything else.
   */
  @Nonnull
  private static Object[] nonNullArguments(@Nonnull final Method method) {
    return Arrays.stream(method.getParameterTypes())
        .map(BuilderNullArgumentTest::nonNullArgument)
        .toArray();
  }

  /** Builds a non-null argument of the given reference type. */
  @Nonnull
  private static Object nonNullArgument(@Nonnull final Class<?> type) {
    return type == String.class ? "test" : mock(type);
  }

  /** Whether a method overrides one declared by {@link Object}, such as a record's equals. */
  private static boolean overridesObjectMethod(@Nonnull final Method method) {
    try {
      Object.class.getMethod(method.getName(), method.getParameterTypes());
      return true;
    } catch (final NoSuchMethodException e) {
      return false;
    }
  }

  /** Describes a parameter of a method, for use as a test name. */
  @Nonnull
  private static String describe(@Nonnull final Method method, final int index) {
    final String parameterTypes =
        Arrays.stream(method.getParameterTypes())
            .map(Class::getSimpleName)
            .reduce((a, b) -> a + ", " + b)
            .orElse("");
    return "%s.%s(%s) parameter %d"
        .formatted(
            method.getDeclaringClass().getSimpleName(), method.getName(), parameterTypes, index);
  }
}
