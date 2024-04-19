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

import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Terminology helper functions
 */
public interface TerminologyUtils {

  /**
   * Checks if a path is a codeable concept element.
   *
   * @param fhirPath a path to check
   * @return true if the path is a codeable concept
   */
  static boolean isCodeableConcept(@Nonnull final FhirPath fhirPath) {
    return (fhirPath instanceof ElementPath &&
        FHIRDefinedType.CODEABLECONCEPT.equals(((ElementPath) fhirPath).getFhirType()));
  }

  /**
   * Checks is a path is a coding or codeable concept path.
   *
   * @param fhirPath a path to check
   * @return true if the path is coding or codeable concept
   */
  static boolean isCodingOrCodeableConcept(@Nonnull final FhirPath fhirPath) {
    if (fhirPath instanceof CodingLiteralPath) {
      return true;
    } else if (fhirPath instanceof ElementPath) {
      final FHIRDefinedType elementFhirType = ((ElementPath) fhirPath).getFhirType();
      return FHIRDefinedType.CODING.equals(elementFhirType)
          || FHIRDefinedType.CODEABLECONCEPT.equals(elementFhirType);
    } else {
      return false;
    }
  }

  @Nonnull
  static Column getCodingColumn(@Nonnull final FhirPath fhirPath) {
    check(isCodingOrCodeableConcept(fhirPath),
        "Coding or CodeableConcept path expected");
    final Column conceptColumn = fhirPath.getValueColumn();
    return isCodeableConcept(fhirPath)
           ? conceptColumn.getField("coding")
           : conceptColumn;
  }
}
