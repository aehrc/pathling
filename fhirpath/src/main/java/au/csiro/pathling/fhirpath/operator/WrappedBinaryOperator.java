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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;

@Value
public class WrappedBinaryOperator implements BinaryOperator {

  Method method;

  @Nonnull
  public Collection invoke(@Nonnull final BinaryOperatorInput operatorInput) {
    // first arguemnt to the method is always the input collection

    // eval arguments
    final Object[] invocationArgs = Stream.of(operatorInput.getLeft(), operatorInput.getRight())
        .toArray(Object[]::new);
    try {
      return (Collection) method.invoke(null, invocationArgs);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static WrappedBinaryOperator of(@Nonnull final Method method) {
    return new WrappedBinaryOperator(method);
  }

  @Nonnull
  public static Map<String, BinaryOperator> mapOf(@Nonnull final Class<?> clazz) {
    return Stream.of(clazz.getDeclaredMethods())
        .filter(m -> m.getAnnotation(FhirPathOperator.class) != null)
        .collect(Collectors.toUnmodifiableMap(Method::getName, WrappedBinaryOperator::new));

  }

}
