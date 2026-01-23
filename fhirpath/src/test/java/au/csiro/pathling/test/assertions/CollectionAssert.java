/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.CollectionDataset;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * @author Piotr Szul
 * @author John Grimes
 */
@SuppressWarnings("unused")
public class CollectionAssert {

  @Nonnull
  protected final CollectionDataset datasetResult;
  protected final Collection result;

  CollectionAssert(@Nonnull final CollectionDataset datasetResult) {
    this.datasetResult = datasetResult;
    this.result = datasetResult.getValue();
  }


  @Nonnull
  public DatasetAssert toCanonicalResult() {
    return DatasetAssert.of(datasetResult.toCanonical().toIdValueDataset());
  }

  @Nonnull
  public DatasetAssert toExternalResult() {
    return DatasetAssert.of(datasetResult.toIdExternalValueDataset());
  }


  public CollectionAssert hasClass(final Class<? extends Collection> ofType) {
    assertTrue(ofType.isAssignableFrom(result.getClass()),
        ofType.getName() + " is not assignable from " + result.getClass().getName());
    return this;
  }

  @Nonnull
  public CollectionAssert hasFhirType(@Nonnull final FHIRDefinedType type) {
    assertEquals(type, result.getFhirType().orElse(null));
    return this;
  }
}
