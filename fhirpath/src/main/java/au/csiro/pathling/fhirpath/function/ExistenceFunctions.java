package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.validation.FhirPathFunction;
import javax.annotation.Nonnull;

/**
 * Contains functions for evaluating the existence of elements in a collection.
 *
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#existence">FHIRPath Specification -
 * Existence</a>
 */
@SuppressWarnings("unused")
public class ExistenceFunctions {

  /**
   * Takes a collection of Boolean values and returns {@code true} if all the items are
   * {@code true}. If any items are {@code false}, the result is {@code false}.
   * <p>
   * If the input is empty, the result is {@code true}.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#alltrue--boolean">FHIRPath Specification
   * - allTrue</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection allTrue(@Nonnull final BooleanCollection input) {
    return BooleanCollection.build(input.getColumn().allTrue());
  }

  /**
   * Takes a collection of Boolean values and returns {@code true} if all the items are
   * {@code false}. If any items are {@code true}, the result is {@code false}.
   * <p>
   * If the input is empty, the result is {@code true}.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#allfalse--boolean">FHIRPath Specification
   * - allFalse</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection allFalse(@Nonnull final BooleanCollection input) {
    return BooleanCollection.build(input.getColumn().allFalse());
  }

  /**
   * Returns {@code true} if any of the items in the input collection are {@code true}.
   * <p>
   * If all items are {@code false}, or if the input is empty, the result is {@code false}.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#anytrue--boolean">FHIRPath Specification
   * - anyTrue</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection anyTrue(@Nonnull final BooleanCollection input) {
    return BooleanCollection.build(input.getColumn().anyTrue());
  }

  /**
   * Returns {@code true} if any of the items in the input collection are {@code false}.
   * <p>
   * If all items are {@code true}, or if the input is empty, the result is {@code false}.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#anyfalse--boolean">FHIRPath Specification
   * - anyFalse</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection anyFalse(@Nonnull final BooleanCollection input) {
    return BooleanCollection.build(input.getColumn().anyFalse());
  }

}
