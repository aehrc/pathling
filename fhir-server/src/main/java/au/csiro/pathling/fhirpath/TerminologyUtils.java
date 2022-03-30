/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import javax.annotation.Nonnull;
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
}
