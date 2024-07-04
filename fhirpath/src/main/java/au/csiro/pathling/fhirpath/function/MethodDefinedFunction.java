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

package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;

/**
 * A {@link NamedFunction} that is defined using a static method.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@Value
public class MethodDefinedFunction implements NamedFunction<Collection> {

  String name;
  Method method;

  @Override
  @Nonnull
  public Collection invoke(@Nonnull final FunctionInput functionInput) {
    final FunctionParameterResolver resolver = new FunctionParameterResolver(
        functionInput.getContext(),
        functionInput.getInput());
    final List<FhirPath> actualArguments = functionInput.getArguments();

    // Resolve each pair of method parameter and argument.
    final Stream<Object> resolvedArguments = IntStream.range(0, method.getParameterCount() - 1)
        .mapToObj(i -> resolver.resolveArgument(method.getParameters()[i + 1],
            (i < actualArguments.size())
            ? actualArguments.get(i)
            : null));

    // Resolve the input.
    final Object input = resolver.resolveCollection(functionInput.getInput(),
        method.getParameters()[0]);

    // Create an array of arguments to pass to the method.
    final Object[] invocationArgs = Stream.concat(Stream.of(input),
        resolvedArguments).toArray(Object[]::new);
    try {
      // Invoke the method and return the result.
      return (Collection) method.invoke(null, invocationArgs);
    } catch (final IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Error invoking method-defined function", e);
    }
  }

  /**
   * Builds a {@link MethodDefinedFunction} from a {@link Method}.
   *
   * @param method The method to build the function from
   * @return A new {@link MethodDefinedFunction}
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
  public static List<NamedFunction<?>> build(@Nonnull final Class<?> clazz) {
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
  public static Map<String, NamedFunction<?>> mapOf(@Nonnull final Class<?> clazz) {
    return build(clazz).stream().collect(Collectors.toUnmodifiableMap(NamedFunction::getName,
        Function.identity()));
  }

}
