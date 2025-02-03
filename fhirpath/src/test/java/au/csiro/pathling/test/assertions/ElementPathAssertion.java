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

package au.csiro.pathling.test.assertions;

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.execution.CollectionDataset;
import jakarta.annotation.Nonnull;

/**
 * @author John Grimes
 */
@SuppressWarnings("UnusedReturnValue")
@NotImplemented
public class ElementPathAssertion extends BaseFhirPathAssertion<ElementPathAssertion> {

  ElementPathAssertion(@Nonnull final CollectionDataset datasetResult) {
    super(datasetResult);
  }

  // TODO: check

  // @Nonnull
  // private final PrimitivePath fhirPath;
  //
  // ElementPathAssertion(@Nonnull final PrimitivePath fhirPath) {
  //   super(fhirPath);
  //   this.fhirPath = fhirPath;
  // }
  //
  // @Nonnull
  // public ElementPathAssertion hasFhirType(@Nonnull final FHIRDefinedType type) {
  //   assertEquals(type, fhirPath.getFhirType());
  //   return this;
  // }
  //
  //
  // @Nonnull
  // public ElementPathAssertion hasDefinition(@Nonnull final ElementDefinition elementDefinition) {
  //   assertTrue(fhirPath.getDefinition().isPresent());
  //   assertEquals(elementDefinition, fhirPath.getDefinition().get());
  //   return this;
  // }
}
