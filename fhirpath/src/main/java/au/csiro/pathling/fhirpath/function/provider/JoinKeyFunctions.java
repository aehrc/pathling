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

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.expression.TypeSpecifier;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.fhirpath.function.annotation.OptionalParameter;
import org.jetbrains.annotations.NotNull;

/**
 * FHIRPath functions for generating keys for joining between resources.
 *
 * @author Piotr Szul
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html#required-additional-functions">SQL
 * on FHIR specification - Required Additional Functions</a>
 */
@SuppressWarnings("unused")
public class JoinKeyFunctions {

  /**
   * Returns a {@link Collection} of keys for the input {@link Collection}.
   *
   * @param input The input {@link Collection}
   * @return A {@link Collection} of keys
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html#getresourcekey--keytype">SQL
   * on FHIR specification - getResourceKey</a>
   */
  @FhirPathFunction
  public static @NotNull Collection getResourceKey(final @NotNull Collection input) {
    throw new RuntimeException("Not implemented");
  }

  /**
   * Returns a {@link Collection} of keys for the input {@link Collection}.
   *
   * @param input The input {@link Collection}
   * @param typeSpecifier An optional {@link TypeSpecifier} to filter the reference keys by
   * @return A {@link Collection} of keys
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html#getreferencekeyresource-type-specifier--keytype">SQL
   * on FHIR specification - getReferenceKey</a>
   */
  @FhirPathFunction
  public static @NotNull Collection getReferenceKey(final @NotNull Collection input,
      final @OptionalParameter TypeSpecifier typeSpecifier) {
    throw new RuntimeException("Not implemented");
  }

}
