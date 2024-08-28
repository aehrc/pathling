package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import au.csiro.pathling.fhirpath.expression.TypeSpecifier;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.fhirpath.function.annotation.RequiredParameter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.jetbrains.annotations.NotNull;

/**
 * Contains functions for filtering and projecting elements in a collection.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/#filtering-and-projection">FHIRPath
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
   * @see <a href="https://hl7.org/fhirpath/#wherecriteria--expression--collection">where</a>
   */
  @FhirPathFunction
  public static Collection where(@NotNull final EvaluationContext context,
      @NotNull final Collection input, @RequiredParameter final FhirPath expression) {
    final Collection expressionResult = expression.evaluate(input, context);
    final Column resultColumn = functions.filter(input.getColumn(),
        c -> expressionResult.getColumn());
    return input.map(i -> resultColumn);
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
  @NotNull
  public static Collection ofType(@NotNull final Collection input,
      @NotNull final TypeSpecifier typeSpecifier) {
    throw new RuntimeException("Not implemented");
  }

}
