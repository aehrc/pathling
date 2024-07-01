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
import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.validation.FhirPathFunction;
import au.csiro.pathling.utilities.Preconditions;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Implementation of standard FHIRPath functions.
 */
@SuppressWarnings("unused")
public class StandardFunctions {

  public static final String EXTENSION_ELEMENT_NAME = "extension";
  public static final String URL_ELEMENT_NAME = "url";
  public static final String JOIN_DEFAULT_SEPARATOR = "";

  public static final String REFERENCE_ELEMENT_NAME = "reference";


  // Maybe these too can be implemented as colum functions????
  @FhirPathFunction
  public static Collection iif(@Nonnull final Collection input,
      @Nonnull CollectionTransform expression, @Nonnull final Collection thenValue,
      @Nonnull final Collection otherwiseValue) {

    // we need to pre-evaluate both expressions to determine that they have the same type
    // this may however cause some issues because we only use one of them (and as such we only need to apply one side effect).

    // functions.when(requireBoolean(expression).apply(input).getSingleton(), thenValue.getColumn())
    //     .otherwise(otherwiseValue.getColumn());

    return EmptyCollection.getInstance();
  }

  /**
   * This function allows the selection of only the first element of a collection.
   *
   * @param input the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#first">first</a>
   */
  @FhirPathFunction
  public static Collection first(@Nonnull final Collection input) {
    return input.copyWith(input.getColumn().first());
  }

  /**
   * A function for aggregating data based on counting the number of rows within the result.
   *
   * @param input the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#count">count</a>
   */
  @FhirPathFunction
  public static IntegerCollection count(@Nonnull final Collection input) {
    return IntegerCollection.build(input.getColumn().count());
  }

  /**
   * A function that returns the extensions of the current element that match a given URL.
   *
   * @author Piotr Szul
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#extension">extension</a>
   */
  @FhirPathFunction
  public static Collection extension(@Nonnull final Collection input,
      @Nonnull final StringCollection url) {
    return input.traverse(EXTENSION_ELEMENT_NAME).map(extensionCollection ->
        FilteringAndProjectionFunctions.where(extensionCollection, c -> c.traverse(URL_ELEMENT_NAME).map(
                urlCollection -> urlCollection.getComparison(EQUALS).apply(url))
            .map(col -> BooleanCollection.build(new DefaultRepresentation(col)))
            .orElse(BooleanCollection.fromValue(false)))
    ).orElse(EmptyCollection.getInstance());
  }

  @FhirPathFunction
  public static StringCollection join(@Nonnull final StringCollection input,
      @Nullable final StringCollection separator) {
    return StringCollection.build(input.getColumn().join(
        nonNull(separator)
        ? separator.asSingular().getColumn()
        : DefaultRepresentation.literal(JOIN_DEFAULT_SEPARATOR)
    ));
  }

  public static boolean isTypeSpecifierFunction(@Nonnull final String functionName) {
    return "ofType".equals(functionName) || "getReferenceKey".equals(functionName);
  }

  // TODO: This should be a string collection with a StringCoercible argument
  @FhirPathFunction
  public static Collection toString(@Nonnull final Collection input) {
    Preconditions.checkUserInput(input instanceof StringCoercible,
        "toString() can only be applied to a StringCoercible path");
    return ((StringCoercible) input).asStringPath();
  }

  private StandardFunctions() {
  }

}
