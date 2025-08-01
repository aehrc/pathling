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

package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;

/**
 * FHIRPath functions for generating keys for joining between resources.
 *
 * @author Piotr Szul
 * @see <a
 * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition.html#required-additional-functions">SQL
 * on FHIR specification - Required Additional Functions</a>
 */
@SuppressWarnings("unused")
public class JoinKeyFunctions {

  private JoinKeyFunctions() {
  }

  /**
   * Returns a {@link Collection} of keys for the input {@link ResourceCollection}.
   *
   * @param input The input {@link ResourceCollection}
   * @return A {@link Collection} of keys
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition.html#getresourcekey--keytype">SQL
   * on FHIR specification - getResourceKey</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.REQUIRED)
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
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition.html#getreferencekeyresource-type-specifier--keytype">SQL
   * on FHIR specification - getReferenceKey</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.REQUIRED)
  @Nonnull
  public static Collection getReferenceKey(@Nonnull final ReferenceCollection input,
      @Nullable final TypeSpecifier typeSpecifier) {
    return input.getKeyCollection(Optional.ofNullable(typeSpecifier));
  }

}
