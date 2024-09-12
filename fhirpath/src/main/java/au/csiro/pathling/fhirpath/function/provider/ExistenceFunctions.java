package au.csiro.pathling.fhirpath.function.provider;

import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.fhirpath.function.annotation.OptionalParameter;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.jetbrains.annotations.NotNull;

/**
 * Contains functions for evaluating the existence of elements in a collection.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/#existence">FHIRPath Specification -
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
   * @see <a href="https://hl7.org/fhirpath/#existscriteria--expression--boolean">FHIRPath
   * Specification - exists</a>
   */
  @FhirPathFunction
  public static @NotNull BooleanCollection exists(final @NotNull EvaluationContext context,
      final @NotNull Collection input, final @OptionalParameter FhirPath criteria) {
    final Collection emptyInput = criteria == null
                                  ? input
                                  : FilteringAndProjectionFunctions.where(context, input, criteria);
    return BooleanLogicFunctions.not(empty(emptyInput));
  }

  /**
   * Returns {@code true} if the input collection is empty and {@code false} otherwise.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://hl7.org/fhirpath/#empty--boolean">FHIRPath Specification -
   * empty</a>
   */
  @FhirPathFunction
  public static BooleanCollection empty(final Collection input) {
    final Column column;
    if (input instanceof EmptyCollection) {
      column = functions.lit(true);
    } else {
      column = when(input.getColumn().isNotNull(), size(input.getColumn()).equalTo(0))
          .otherwise(true);
    }
    return new BooleanCollection(column, Optional.of(FhirPathType.BOOLEAN));
  }

}
