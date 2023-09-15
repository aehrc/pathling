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

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import javax.annotation.Nonnull;

import static au.csiro.pathling.fhirpath.function.CollectionExpression.*;

public class StandardFunctions {


  @Nonnull
  @FhirpathFunction
  public static Collection where(@Nonnull final Collection input,
      @Nonnull CollectionExpression expression) {
    return input.copyWith(
        input.getCtx().filter(expression.requireBoolean().toColumnFunction(input)));
  }
  //
  //
  // // Maybe these too can be implemented as colum functions????
  // @FhirpathFunction
  // public Collection iif(@Nonnull final Collection input,
  //     @Nonnull CollectionExpression expression, @Nonnull Collection thenValue,
  //     @Nonnull Collection otherwiseValue) {
  //   // if we do not need to modify the context then maybe enough to just pass the bound expressions
  //   // (but in fact it's lazy eval anyway and at some point we should check
  //   functions.when(requireBoolean(expression).apply(input).getSingleton(), thenValue.getColumn())
  //       .otherwise(otherwiseValue.getColumn());
  //   // we need to check that the result of the expression is boolean
  //   return Collection.nullCollection();
  // }

  @FhirpathFunction
  public static Collection first(@Nonnull final Collection input) {
    return input.copyWith(input.getCtx().first());
  }

  @FhirpathFunction
  public static BooleanCollection empty(@Nonnull final Collection input) {
    return BooleanCollection.build(input.getCtx().empty());
  }

  @FhirpathFunction
  public static IntegerCollection count(@Nonnull final Collection input) {
    return IntegerCollection.build(input.getCtx().count());
  }

}
