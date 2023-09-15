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

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import lombok.Value;
import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    public Object resolveArgument(@Nonnull final Parameter parameter,
        FhirPath<Collection, Collection> argument) {
      if (Collection.class.isAssignableFrom(parameter.getType())) {
        // evaluate collection types 
        return argument.apply(input, evaluationContext);
      } else if (CollectionExpression.class.isAssignableFrom(parameter.getType())) {
        // bind with context
        return (CollectionExpression)(c -> argument.apply(c, evaluationContext));
      } else {
        throw  new RuntimeException("Cannot resolve parameter:" + parameter);
      }
    }
  }

  @Override
  @Nonnull
  public Collection invoke(@Nonnull final FunctionInput functionInput) {
    // first arguemnt to the method is always the input collection

    final ParamResolver resolver = new ParamResolver(functionInput.getContext(),
        functionInput.getInput());

    final Stream<Object> resolvedArguments = IntStream.range(0, method.getParameterCount() - 1)
        .mapToObj(i -> resolver.resolveArgument(method.getParameters()[i + 1],
            functionInput.getArguments().get(i)));
    // eval arguments
    final Object[] invocationArgs = Stream.concat(Stream.of(functionInput.getInput()),
        resolvedArguments).toArray(Object[]::new);
    try {
      return (Collection) method.invoke(null, invocationArgs);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static WrappedFunction of(@Nonnull final Method method) {
    return new WrappedFunction(method.getName(), method);
  }

  @Nonnull
  public static List<NamedFunction> of(@Nonnull final Class<?> clazz) {
    return Stream.of(clazz.getDeclaredMethods())
        .filter(m -> m.getAnnotation(FhirpathFunction.class) != null)
        .map(WrappedFunction::of).collect(Collectors.toUnmodifiableList());
  }

  @Nonnull
  public static Map<String, NamedFunction> mapOf(@Nonnull final Class<?> clazz) {
    return of(clazz).stream().collect(Collectors.toUnmodifiableMap(NamedFunction::getName,
        Function.identity()));
  }

}
