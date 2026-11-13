/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.helpers;

import au.csiro.pathling.fhirpath.evaluation.CollectionDataset;
import jakarta.annotation.Nonnull;

/**
 * Takes the result of an evaluation as the values it hands on. A primitive reached by traversal is
 * held during evaluation as a structure of its value, its id and extensions, and its annotations
 * (decisions 152 and 154), and the engine hands it on as its System value. A test that reads the
 * values of a result, rather than the form evaluation holds them in, reads them in that form.
 */
public final class PrimitiveValues {

  private PrimitiveValues() {}

  /**
   * Returns a result with its primitives as the System values they convert to, and any other
   * collection as it is.
   *
   * @param result the result of an evaluation
   * @return the result, with its primitives as their System values
   */
  @Nonnull
  public static CollectionDataset of(@Nonnull final CollectionDataset result) {
    return CollectionDataset.of(result.getDataset(), result.getValue().withoutPrimitiveMetadata());
  }
}
