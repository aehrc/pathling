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

import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.validation.FhirPathFunction;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * FHIRPath functions for generating keys for joining between resources.
 *
 * @author Piotr Szul
 */
@SuppressWarnings("unused")
public class JoinKeyFunctions {


  /**
   * Returns a {@link Collection} of keys for the input {@link ResourceCollection}.
   *
   * @param input The input {@link ResourceCollection}
   * @return A {@link Collection} of keys
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html#getresourcekey--keytype">getResourceKey</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection getResourceKey(@Nonnull final ResourceCollection input) {
    return input.getKeyCollection();
  }

  /**
   * Returns a {@link Collection} of keys for the input {@link ReferenceCollection}.
   *
   * @param input The input {@link ReferenceCollection}
   * @param typeSpecifier An optional {@link TypeSpecifier} to filter the reference keys by
   * @return A {@link Collection} of keys
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html#getreferencekeyresource-type-specifier--keytype">getReferenceKey</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection getReferenceKey(@Nonnull final ReferenceCollection input,
      @Nullable final TypeSpecifier typeSpecifier) {
    return input.getKeyCollection(Optional.ofNullable(typeSpecifier));
  }

}
