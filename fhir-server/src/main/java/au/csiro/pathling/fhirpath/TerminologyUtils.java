package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Terminology helper funtions
 */
public interface TerminologyUtils {

  /**
   * Checks is a path is a coding or codeable concept path.
   *
   * @param fhirPath a path to check
   * @return true is a path is coding or codeable concept.
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
