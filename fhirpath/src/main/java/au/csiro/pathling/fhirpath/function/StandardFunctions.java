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

import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.MixedCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
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

  /**
   * Describes a function which can scope down the previous invocation within a FHIRPath expression,
   * based upon an expression passed in as an argument. Supports the use of `$this` to reference the
   * element currently in scope.
   *
   * @param input the input collection
   * @param expression the expression to evaluate
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#where">where</a>
   */
  @Nonnull
  @FhirpathFunction
  public static Collection where(@Nonnull final Collection input,
      @Nonnull final CollectionExpression expression) {
    return input.filter(expression.requireBoolean().toColumnFunction(input));
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

  /**
   * This function allows the selection of only the first element of a collection.
   *
   * @param input the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#first">first</a>
   */
  @FhirpathFunction
  public static Collection first(@Nonnull final Collection input) {
    return input.copyWith(input.getCtx().first());
  }

  /**
   * This function returns true if the input collection is empty.
   *
   * @param input the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#empty">empty</a>
   */
  @FhirpathFunction
  public static BooleanCollection empty(@Nonnull final Collection input) {
    return BooleanCollection.build(input.getCtx().empty());
  }

  /**
   * A function for aggregating data based on counting the number of rows within the result.
   *
   * @param input the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#count">count</a>
   */
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

  /**
   * A function filters items in the input collection to only those that are of the given type.
   *
   * @param input the input collection
   * @param typeSpecifier the type specifier
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#oftype">ofType</a>
   */
  @FhirpathFunction
  public static Collection ofType(@Nonnull final MixedCollection input,
      @Nonnull final TypeSpecifier typeSpecifier) {
    // TODO: This should work on any collection type - not just mixed
    // if the type of the collection does not match the required type then it should return an empty collection.
    return input.resolveChoice(typeSpecifier.toFhirType().toCode())
        .orElse(Collection.nullCollection());
  }

  @FhirpathFunction
  public static StringCollection join(@Nonnull final StringCollection input,
      @Nullable final StringCollection separator) {
    return StringCollection.build(input.getCtx().join(
        nonNull(separator)
        ? separator.toLiteralValue()
        : JOIN_DEFAULT_SEPARATOR
    ));
  }

  /**
   * A function which is able to test whether the input collection is empty. It can also optionally
   * accept an argument which can filter the input collection prior to applying the test.
   *
   * @param input the input collection
   * @param criteria the criteria to apply to the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#exists">exists</a>
   */
  @FhirpathFunction
  public static BooleanCollection exists(@Nonnull final Collection input,
      @Nullable final CollectionExpression criteria) {
    return not(empty(nonNull(criteria)
                     ? where(input, criteria)
                     : input));

  }

  /**
   * Returns {@code true} if the input collection evaluates to {@code false}, and {@code false} if
   * it evaluates to {@code true}. Otherwise, the result is empty.
   *
   * @param input the input collection
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#not">not</a>
   */
  @FhirpathFunction
  public static BooleanCollection not(@Nonnull final BooleanCollection input) {
    return BooleanCollection.build(input.getCtx().not());
  }

  public static boolean isTypeSpecifierFunction(@Nonnull final String functionName) {
    return "ofType".equals(functionName) || "getReferenceKey".equals(functionName);
  }

}
