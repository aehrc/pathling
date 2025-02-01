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

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Terminology helper functions
 */
public interface TerminologyUtils {

  /**
   * Checks if a path is a codeable concept element.
   *
   * @param result a path to check
   * @return true if the path is a codeable concept
   */
  static boolean isCodeableConcept(@Nonnull final Collection result) {
    return result.getFhirType().map(FHIRDefinedType.CODEABLECONCEPT::equals).orElse(false);
  }

  /**
   * Checks if a path is a coding path
   *
   * @param result a path to check
   * @return true if the path is a codeable concept
   */
  static boolean isCoding(@Nonnull final Collection result) {
    return result instanceof CodingCollection ||
        result.getFhirType().map(FHIRDefinedType.CODING::equals).orElse(false);
  }

  /**
   * Checks is a path is a coding or codeable concept path.
   *
   * @param result a path to check
   * @return true if the path is coding or codeable concept
   */
  static boolean isCodingOrCodeableConcept(@Nonnull final Collection result) {
    return isCoding(result) || isCodeableConcept(result);
  }

}
