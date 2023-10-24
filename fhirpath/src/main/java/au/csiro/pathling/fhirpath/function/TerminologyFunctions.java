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
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.ColumnCtx;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class TerminologyFunctions {


  /**
   * This function returns the display name for given Coding
   *
   * @author Piotr Szul
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#display">display</a>
   */
  @FhirpathFunction
  public static StringCollection display(@Nonnull final CodingCollection input,
      @Nullable final StringCollection language) {

    return StringCollection.build(input.getCtx()
        .mapWithUDF("display", Optional.ofNullable(language)
            .map(StringCollection::getCtx)
            .map(ColumnCtx::singular)
            .orElse(ColumnCtx.nullCtx()))
        .removeNulls()
    );
  }

  /**
   * This function returns the value of a property for a Coding.
   *
   * @author Piotr Szul
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#property">property</a>
   */
  @FhirpathFunction
  public static Collection property(@Nonnull final CodingCollection input,
      @Nonnull final StringCollection code,
      @Nullable final StringCollection type,
      @Nullable final StringCollection language) {
    // OK: This is actually tricky because the type needs to be a literal. 
    // It would be better if it was type specifier (but literal can be incorporated as well)
    throw new UnsupportedOperationException("Not implemented: property()");
  }

  /**
   * This function returns the designations of a Coding.
   *
   * @author Piotr Szul
   * @see <a
   * href="https://pathling.csiro.au/docs/fhirpath/functions.html#designation">designation</a>
   */
  @FhirpathFunction
  public static StringCollection designation(@Nonnull final CodingCollection input,
      @Nullable final CodingCollection use,
      @Nullable final StringCollection language) {

    return StringCollection.build(input.getCtx()
        .mapWithUDF("designation",
            Optional.ofNullable(use)
                .map(CodingCollection::getCtx)
                .map(ColumnCtx::singular)
                .orElse(ColumnCtx.nullCtx()),
            Optional.ofNullable(language)
                .map(StringCollection::getCtx)
                .map(ColumnCtx::singular)
                .orElse(ColumnCtx.nullCtx())
        )
        .flatten().removeNulls()
    );
  }

  /**
   * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of boolean
   * values, based upon whether each item is present within the ValueSet identified by the supplied
   * URL.
   *
   * @author John Grimes
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#memberof">memberOf</a>
   */
  @FhirpathFunction
  public static BooleanCollection memberOf(@Nonnull final CodingCollection input,
      @Nonnull final StringCollection valueSetURL) {
    return BooleanCollection.build(
        input.getCtx().callUDF("member_of", valueSetURL.getCtx().singular())
    );
  }
}
