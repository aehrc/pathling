package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.ColumnTransform;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Contains functions for filtering and projecting elements in a collection.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#filtering-and-projection">FHIRPath
 * Specification - Filtering and projection</a>
 */
public class FilteringAndProjectionFunctions {

  /**
   * Returns a collection containing only those elements in the input collection for which the
   * stated criteria expression evaluates to {@code true}. Elements for which the expression
   * evaluates to {@code false} or empty are not included in the result.
   * <p>
   * If the input collection is empty, the result is empty.
   * <p>
   * If the result of evaluating the condition is other than a single boolean value, the evaluation
   * will end and signal an error to the calling environment, consistent with singleton evaluation
   * of collections behavior.
   *
   * @param input The input collection
   * @param expression The criteria expression
   * @return A collection containing only those elements for which the criteria expression evaluates
   * to {@code true}
   * @see <a
   * href="https://build.fhir.org/ig/HL7/FHIRPath/#wherecriteria--expression--collection">where</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection where(@Nonnull final Collection input,
      @Nonnull final CollectionTransform expression) {
    return input.filter(expression.requireBoolean().toColumnTransformation(input));
  }

  /**
   * Evaluates the projection expression for each item in the input collection. The result of each
   * evaluation is added to the output collection. <p If the evaluation results in a collection with
   * multiple items, all items are added to the output collection (collections resulting from
   * evaluation of projection are flattened).
   * <p>
   * This means that if the evaluation for an element results in the empty collection ({ }), no
   * element is added to the result, and that if the input collection is empty ({ }), the result is
   * empty as well.
   *
   * @param input The input collection
   * @param mapper The projection expression
   * @return A collection containing the results of evaluating the projection expression for each
   * item in the input collection
   * @see <a
   * href="https://build.fhir.org/ig/HL7/FHIRPath/#selectprojection-expression--collection">select</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection select(@Nonnull final Collection input,
      @Nonnull final CollectionTransform mapper) {
    final ColumnTransform columnMapper = mapper.toColumnTransformation(input);
    return mapper.apply(input)
        .copyWith(
            input
                .getColumn()
                .transform(
                    // TODO: this is not correct as maybe the representation should not be 
                    //  a DefaultRepresentation but a different one
                    //  this needs to be fixed in a few places 
                    col -> columnMapper.apply(new DefaultRepresentation(col)).getValue()
                )
                .flatten()
        );
  }

  /**
   * Returns a collection that contains all items in the input collection that are of the given type
   * or a subclass thereof. If the input collection is empty, the result is empty. The type argument
   * is an identifier that must resolve to the name of a type in a model. For implementations with
   * compile-time typing, this requires special-case handling when processing the argument to treat
   * it as type specifier rather than an identifier expression.
   *
   * @param input The input collection
   * @param typeSpecifier The type specifier
   * @return A collection containing only those elements that are of the specified type
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#oftype">ofType</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection ofType(@Nonnull final Collection input,
      @Nonnull final TypeSpecifier typeSpecifier) {
    return input.filterByType(typeSpecifier);
  }

}
