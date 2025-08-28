/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.resolver.FunctionParameterResolver;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link NamedFunction} that is defined using a static method.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public record MethodDefinedFunction(String name, Method method) implements
    NamedFunction {

  @Override
  @Nonnull
  public Collection invoke(@Nonnull final FunctionInput functionInput) {
    return FunctionParameterResolver.fromFunctionInput(functionInput)
        .bind(method).invoke();
  }

  /**
   * Builds a MethodDefinedFunction from a {@link Method}.
   *
   * @param method The method to build the function from
   * @return A new MethodDefinedFunction
   */
  @Nonnull
  public static MethodDefinedFunction build(@Nonnull final Method method) {
    return new MethodDefinedFunction(method.getName(), method);
  }

  /**
   * Builds a list of {@link NamedFunction}s from the methods defined within a class.
   *
   * @param clazz The class to build the functions from
   * @return A list of {@link NamedFunction}s
   */
  @Nonnull
  public static List<NamedFunction> build(@Nonnull final Class<?> clazz) {
    return Stream.of(clazz.getDeclaredMethods())
        .filter(m -> m.getAnnotation(FhirPathFunction.class) != null)
        .map(MethodDefinedFunction::build).collect(Collectors.toUnmodifiableList());
  }

  /**
   * Builds a map of {@link NamedFunction}s from the methods defined within a class.
   *
   * @param clazz The class to build the functions from
   * @return A map of {@link NamedFunction}s
   */
  @Nonnull
  public static Map<String, NamedFunction> mapOf(@Nonnull final Class<?> clazz) {
    return build(clazz).stream().collect(Collectors.toUnmodifiableMap(NamedFunction::name,
        Function.identity()));
  }

}
