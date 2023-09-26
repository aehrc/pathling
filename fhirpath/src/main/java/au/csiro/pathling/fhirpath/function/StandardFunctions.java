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

import static au.csiro.pathling.fhirpath.Comparable.ComparisonOperation.EQUALS;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.MixedCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import javax.annotation.Nonnull;

@SuppressWarnings("unused")
public class StandardFunctions {

  public static final String EXTENSION_ELEMENT_NAME = "extension";
  public static final String URL_ELEMENT_NAME = "url";

  @Nonnull
  @FhirpathFunction
  public static Collection where(@Nonnull final Collection input,
      @Nonnull final CollectionExpression expression) {
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

  /**
   * A function that returns the extensions of the current element that match a given URL.
   *
   * @author Piotr Szul
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#extension">extension</a>
   */
  @FhirpathFunction
  public static Collection extension(@Nonnull final Collection input,
      @Nonnull final StringCollection url) {
    return input.traverse(EXTENSION_ELEMENT_NAME).map(extensionCollection ->
        where(extensionCollection, c -> c.traverse(URL_ELEMENT_NAME).map(
                urlCollection -> urlCollection.getComparison(EQUALS).apply(url))
            .map(BooleanCollection::build).orElse(BooleanCollection.falseCollection()))
    ).orElse(Collection.nullCollection());
  }

  @FhirpathFunction
  public static Collection ofType(@Nonnull final MixedCollection input,
      @Nonnull final Collection typeSpecifier) {
    // TODO: implement as annotation or collection type
    checkUserInput(typeSpecifier.getType().isPresent() && FhirPathType.TYPE_SPECIFIER.equals(
            typeSpecifier.getType().get()),
        "Argument to ofType function must be a type specifier");
    // TODO: This should work on any collection type - not just mixed
    // if the type of the collection does not match the required type then it should return an empty collection.
    return input.resolveChoice(typeSpecifier.getFhirType().get().toCode())
        .orElse(Collection.nullCollection());
  }

  @FhirpathFunction
  public static StringCollection getResourceKey(@Nonnull final ResourceCollection input) {
    return StringCollection.build(input.getKeyColumn().getValue());
  }
}
