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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import au.csiro.pathling.fhirpath.function.annotation.OptionalParameter;
import au.csiro.pathling.fhirpath.function.annotation.RequiredParameter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Value;
import org.jetbrains.annotations.NotNull;

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
  @NotNull
  public Collection invoke(@NotNull final FunctionInput functionInput) {
    final List<FhirPath> actualArguments = functionInput.getArguments();
    final List<Object> invocationArguments = new ArrayList<>(
        Collections.nCopies(method.getParameterCount(), (Object) null));
    ;

    int parameterIndex = 0;
    int argumentIndex = 0;
    for (final Parameter parameter : method.getParameters()) {
      if (EvaluationContext.class.isAssignableFrom(parameter.getType())) {
        // If the parameter is an EvaluationContext, provide the context to the function.
        invocationArguments.set(parameterIndex, functionInput.getContext());

      } else if (Collection.class.isAssignableFrom(parameter.getType())) {
        // If the parameter is a Collection, provide the input to the function.
        invocationArguments.set(parameterIndex, functionInput.getInput());

      } else {
        final FhirPath argument = actualArguments.get(argumentIndex);
        // Check the type of the argument.
        if (!parameter.getType().isAssignableFrom(argument.getClass())) {
          throw new IllegalArgumentException(
              "Invalid argument type for " + parameter.getName() + " parameter passed to " + name
                  + " function: " + parameter.getType());
        }

        if (parameter.getAnnotation(RequiredParameter.class) != null) {
          // If the parameter is a required parameter, provide the corresponding argument.
          try {
            invocationArguments.set(parameterIndex, argument);
          } catch (final IndexOutOfBoundsException e) {
            // If the argument is missing, throw an exception.
            throw new IllegalArgumentException(
                "Missing required parameter for " + name + " function: " + parameter.getName());
          }
          argumentIndex++;

        } else if (parameter.getAnnotation(OptionalParameter.class) != null) {
          // If the parameter is an optional parameter, provide the corresponding argument if it exists.
          try {
            invocationArguments.set(parameterIndex, actualArguments.get(argumentIndex));
          } catch (final IndexOutOfBoundsException e) {
            // If the argument is missing, provide a null value.
            invocationArguments.set(parameterIndex, null);
          }
          argumentIndex++;
        }
      }
      parameterIndex++;
    }

    // Create an array of arguments to pass to the method.
    try {
      // Invoke the method and return the result.
      return (Collection) method.invoke(null, invocationArguments.toArray());
    } catch (final IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Error invoking method-defined function", e);
    }
  }

  /**
   * Builds a MethodDefinedFunction from a {@link Method}.
   *
   * @param method The method to build the function from
   * @return A new MethodDefinedFunction
   */
  @NotNull
  public static MethodDefinedFunction build(@NotNull final Method method) {
    return new MethodDefinedFunction(method.getName(), method);
  }

  /**
   * Builds a list of {@link NamedFunction}s from the methods defined within a class.
   *
   * @param clazz The class to build the functions from
   * @return A list of {@link NamedFunction}s
   */
  @NotNull
  public static List<NamedFunction<?>> build(@NotNull final Class<?> clazz) {
    return Stream.of(clazz.getDeclaredMethods())
        .filter(m -> m.getAnnotation(FhirPathFunction.class) != null)
        .map(MethodDefinedFunction::build).collect(toUnmodifiableList());
  }

  /**
   * Builds a map of {@link NamedFunction}s from the methods defined within a class.
   *
   * @param clazz The class to build the functions from
   * @return A map of {@link NamedFunction}s
   */
  @NotNull
  public static Map<String, NamedFunction<?>> mapOf(@NotNull final Class<?> clazz) {
    return build(clazz).stream().collect(toUnmodifiableMap(NamedFunction::getName,
        identity()));
  }

}
