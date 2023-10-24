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

import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.ColumnCtx;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

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
        .callUDF("display", Optional.ofNullable(language)
            .map(StringCollection::getCtx)
            .map(ColumnCtx::singular)
            .orElse(ColumnCtx.nullCtx()))
        .removeNulls()
    );
  }

}
