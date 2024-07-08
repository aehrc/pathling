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

package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

/**
 * A {@link BinaryOperator} that is defined using a static method.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@Value
public class MethodDefinedOperator implements BinaryOperator {

  Method method;

  @Override
  @Nonnull
  public Collection invoke(@Nonnull final BinaryOperatorInput operatorInput) {
    // Create an array of arguments to pass to the method.
    final Object[] invocationArgs = Stream.of(operatorInput.getLeft(), operatorInput.getRight())
        .toArray(Object[]::new);
    try {
      // Invoke the method.
      return (Collection) method.invoke(null, invocationArgs);
    } catch (final IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Error invoking method-defined operator", e);
    }
  }

  /**
   * Builds a {@link MethodDefinedOperator} from a {@link Method}.
   *
   * @param method The method to build the operator from
   * @return A {@link MethodDefinedOperator}
   */
  @Nonnull
  public static MethodDefinedOperator build(@Nonnull final Method method) {
    return new MethodDefinedOperator(method);
  }

  /**
   * Builds a map of {@link BinaryOperator}s from the methods defined within a class.
   *
   * @param clazz The class to build the operators from
   * @return A map of {@link BinaryOperator}s
   */
  @Nonnull
  public static Map<String, BinaryOperator> mapOf(@Nonnull final Class<?> clazz) {
    return Stream.of(clazz.getDeclaredMethods())
        .filter(m -> m.getAnnotation(FhirPathOperator.class) != null)
        .collect(Collectors.toUnmodifiableMap(Method::getName, MethodDefinedOperator::new));

  }

}
