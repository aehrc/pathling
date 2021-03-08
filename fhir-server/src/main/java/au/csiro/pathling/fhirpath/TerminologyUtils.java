package au.csiro.pathling.fhirpath;

import static java.util.function.Predicate.not;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Terminology helper functions
 */
public interface TerminologyUtils {

  /**
   * Leniently parses a list of coma separated values. Trims the code strings and filters out empty
   * values.
   *
   * @param csvList a coma separated list of equivalence codes
   * @param converter a function that converts single value string to the desired type T
   * @param <T> the type of elements to produce.
   * @return the list of converted values of type T.
   */
  @Nonnull
  static <T> List<T> parseCsvList(@Nonnull final String csvList, final @Nonnull
      Function<String, T> converter) {
    return Stream.of(csvList.split(",")).map(String::trim).filter(not(String::isEmpty))
        .map(converter).collect(
            Collectors.toList());
  }

  /**
   * Checks if a path if a codeable concept element.
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
