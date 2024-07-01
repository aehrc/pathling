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

import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.collection.Collection;
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

  public static final String JOIN_DEFAULT_SEPARATOR = "";

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
