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

import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import au.csiro.pathling.fhirpath.validation.ReturnType;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

@Value
public class ColumnFunction0 implements NamedFunction<Collection> {

  String name;
  Function<Column, Column> columnFunction;
  Optional<FHIRDefinedType> returnType;


  @Override
  @Nonnull
  public String getName() {
    return name;
  }


  @Override
  @Nonnull
  public Collection invoke(@Nonnull final FunctionInput functionInput) {
    final Column columnResult = columnFunction.apply(functionInput.getInput().getColumn());
    // if the result type is not defined pass  the input collection type through
    return returnType.map(rt -> Collection.build(columnResult, rt))
        .orElse((functionInput.getInput().copyWith(columnResult)));
  }

  public static Function<Column, Column> from(Method method) {
    return column -> {
      try {
        return (Column) method.invoke(null, column);
      } catch (final ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    };
  }

  public static ColumnFunction0 of(@Nonnull final Method method) {

    return new ColumnFunction0(method.getName(), from(method),
        Optional.ofNullable(method.getAnnotation(ReturnType.class)).map(ReturnType::value));
  }


  @Nonnull
  public static List<NamedFunction> of(@Nonnull final Class<?> clazz) {
    return Stream.of(clazz.getDeclaredMethods())
        .filter(m -> m.getAnnotation(FhirpathFunction.class) != null)
        .map(ColumnFunction0::of).collect(Collectors.toUnmodifiableList());
  }

  @Nonnull
  public static Map<String, NamedFunction> mapOf(@Nonnull final Class<?> clazz) {
    return of(clazz).stream().collect(Collectors.toUnmodifiableMap(NamedFunction::getName,
        Function.identity()));
  }

}
