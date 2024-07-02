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

import static java.util.Objects.isNull;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.path.Paths;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

@Value
public class WrappedFunction implements NamedFunction<Collection> {

  String name;
  Method method;

  @Override
  @Nonnull
  public String getName() {
    return name;
  }


  @Value
  private static class ParamResolver {

    EvaluationContext evaluationContext;
    Collection input;

    @Nullable
    public Object resolveArgument(@Nonnull final Parameter parameter,
        final FhirPath argument) {

      if (isNull(argument)) {
        // check the pararmeter is happy with a null value
        if (EvaluationContext.class.isAssignableFrom(parameter.getType())) {
          // bind type specifier
          return evaluationContext;
        } else if (parameter.getAnnotation(Nullable.class) != null) {
          return null;
        } else {
          throw new RuntimeException(
              "Parameter " + parameter + " is not nullable and no argument was provided");
        }
        // return Optional.ofNullable(parameter.getAnnotation(Nullable.class))
        //     .map(__ -> null).orElseThrow(() -> new RuntimeException(
        //         "Parameter " + parameter + " is not nullable and no argument was provided"));
      } else if (Collection.class.isAssignableFrom(parameter.getType())) {
        // evaluate collection types 
        return resolveCollection(argument.apply(input, evaluationContext), parameter);
      } else if (CollectionTransform.class.isAssignableFrom(parameter.getType())) {
        // bind with context
        return (CollectionTransform) (c -> argument.apply(c, evaluationContext));
      } else if (TypeSpecifier.class.isAssignableFrom(parameter.getType())) {
        // bind type specifier
        return ((Paths.TypeSpecifierPath) argument).getTypeSpecifier();
      } else if (FhirPath.class.isAssignableFrom(parameter.getType())) {
        // bind type specifier
        return argument;
      } else {
        throw new RuntimeException("Cannot resolve parameter:" + parameter);
      }
    }
  }

  public static Object resolveCollection(@Nonnull final Collection collection,
      @Nonnull final Parameter parameter) {
    if (CodingCollection.class.isAssignableFrom(parameter.getType())) {
      // evaluate collection types 
      return collection.asCoding().orElseThrow();
    } else if (Collection.class.isAssignableFrom(parameter.getType())) {
      // evaluate collection types 
      return collection;
    } else {
      throw new RuntimeException("Cannot resolve input:" + parameter);
    }
  }

  @Override
  @Nonnull
  public Collection invoke(@Nonnull final FunctionInput functionInput) {
    // first arguemnt to the method is always the input collection

    final ParamResolver resolver = new ParamResolver(functionInput.getContext(),
        functionInput.getInput());

    final List<FhirPath> actualArguments = functionInput.getArguments();

    // TODO: make it nicer
    final Stream<Object> resolvedArguments = IntStream.range(0, method.getParameterCount() - 1)
        .mapToObj(i -> resolver.resolveArgument(method.getParameters()[i + 1],
            (i < actualArguments.size())
            ? actualArguments.get(i)
            : null));
    // eval arguments

    // we also may need to map the input here ....

    final Object input = resolveCollection(functionInput.getInput(), method.getParameters()[0]);

    final Object[] invocationArgs = Stream.concat(Stream.of(input),
        resolvedArguments).toArray(Object[]::new);
    try {
      return (Collection) method.invoke(null, invocationArgs);
    } catch (final IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static WrappedFunction of(@Nonnull final Method method) {
    return new WrappedFunction(method.getName(), method);
  }

  @Nonnull
  public static List<NamedFunction<?>> of(@Nonnull final Class<?> clazz) {
    return Stream.of(clazz.getDeclaredMethods())
        .filter(m -> m.getAnnotation(FhirPathFunction.class) != null)
        .map(WrappedFunction::of).collect(Collectors.toUnmodifiableList());
  }

  @Nonnull
  public static Map<String, NamedFunction<?>> mapOf(@Nonnull final Class<?> clazz) {
    return of(clazz).stream().collect(Collectors.toUnmodifiableMap(NamedFunction::getName,
        Function.identity()));
  }

}
