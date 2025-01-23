package au.csiro.pathling.fhirpath.function.provider;

import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Contains functions for evaluating the existence of elements in a collection.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#existence">FHIRPath Specification -
 * Existence</a>
 */
@SuppressWarnings("unused")
public class ExistenceFunctions {

  /**
   * Returns {@code true} if the input collection has any elements (optionally filtered by the
   * criteria), and {@code false} otherwise. This is the opposite of {@code empty()}, and as such is
   * a shorthand for {@code empty().not()}. If the input collection is empty, the result is
   * {@code false}.
   * <p>
   * Using the optional criteria can be considered a shorthand for
   * {@code where(criteria).exists()}.
   *
   * @param input The input collection
   * @param criteria The criteria to apply to the input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a
   * href="https://build.fhir.org/ig/HL7/FHIRPath/#existscriteria--expression--boolean">FHIRPath
   * Specification - exists</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection exists(@Nonnull final Collection input,
      @Nullable final CollectionTransform criteria) {
    return BooleanLogicFunctions.not(empty(nonNull(criteria)
                                           ? FilteringAndProjectionFunctions.where(input, criteria)
                                           : input));

  }

  /**
   * Returns {@code true} if the input collection is empty and {@code false} otherwise.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#empty--boolean">FHIRPath Specification -
   * empty</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection empty(@Nonnull final Collection input) {
    return BooleanCollection.build(input.getColumn().empty());
  }

  /**
   * Returns the integer count of the number of items in the input collection. Returns 0 when the
   * input collection is empty.
   *
   * @param input The input collection
   * @return An {@link IntegerCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#count--integer">FHIRPath Specification -
   * count</a>
   */
  @FhirPathFunction
  @Nonnull
  public static IntegerCollection count(@Nonnull final Collection input) {

    // TODO: Review if this is really necessary and correct
    return IntegerCollection.buildUnsigned(
        input instanceof ResourceCollection
        ? input.getColumn().traverse("id").countDistinct()
        : input.getColumn().count()
    );
  }


  /**
   * Returns the sum of the numbers input collection. Returns 0 when the input collection is empty.
   *
   * @param input The input collection
   * @return An {@link Collection} containing the result count</a>
   */
  // TODO: Update documentation and move to a separate class
  @FhirPathFunction
  @Nonnull
  public static Collection sum(@Nonnull final Collection input) {
    return input.map(ColumnRepresentation::sum);
  }

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
