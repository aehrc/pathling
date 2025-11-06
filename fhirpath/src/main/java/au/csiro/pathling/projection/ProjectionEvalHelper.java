package au.csiro.pathling.projection;


import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import java.util.List;
import java.util.function.UnaryOperator;

import static au.csiro.pathling.encoders.ColumnFunctions.structProduct;

/**
 * Helper class for evaluating projection clauses in various contexts.
 * <p>
 * This record encapsulates a list of projection clauses and provides methods to:
 * <ul>
 *   <li>Convert projection evaluation into a column operator for use in transformations</li>
 *   <li>Evaluate projections for each element in a collection</li>
 *   <li>Determine the result schema without evaluating actual data</li>
 * </ul>
 * </p>
 * <p>
 * This helper is particularly useful in scenarios like {@link RepeatSelection} where
 * projections need to be applied recursively to nested structures or evaluated multiple
 * times with different input contexts.
 * </p>
 *
 * @param projectionClauses the list of projection clauses to evaluate
 * @author Piotr Szul
 */
record ProjectionEvalHelper(@Nonnull List<ProjectionClause> projectionClauses) {

  /**
   * Creates a column operator that evaluates all projection clauses on a given column.
   * <p>
   * The returned operator evaluates each projection clause using the provided column as
   * input context and combines the results into a single struct column. This is useful
   * for creating reusable projection transformations.
   * </p>
   *
   * @param context the projection context containing execution environment
   * @return a unary operator that takes a column and returns a struct containing all
   *     projection results
   */
  @Nonnull
  public UnaryOperator<Column> asColumnOperator(@Nonnull final ProjectionContext context) {
    return c -> {
      // Create a new projection for the element
      final ProjectionContext elementContext = context.withInputColumn(c);
      // Evaluate each of the components of the unnesting selection, and get the result columns.
      final Column[] subSelectionColumns = projectionClauses.stream()
          .map(s -> s.evaluate(elementContext).getResultColumn())
          .toArray(Column[]::new);
      // Combine the result columns into a struct.
      return structProduct(subSelectionColumns);
    };
  }

  /**
   * Evaluates the projection clauses for each element in the input context's collection.
   * <p>
   * This method transforms each element in the input collection by evaluating all projection
   * clauses on it, then flattens the results into a single array. The transformation is
   * applied using the collection's transform and flatten operations.
   * </p>
   *
   * @param context the projection context containing the input collection
   * @return a column containing a flattened array of projection results for all elements
   */
  @Nonnull
  public Column evalForEach(@Nonnull final ProjectionContext context) {
    return context.inputContext().getColumn().transform(
        asColumnOperator(context)
    ).flatten().getValue();
  }

  /**
   * Determines the result schema of the projection clauses without evaluating actual data.
   * <p>
   * This method creates a stub context with empty data and evaluates the projection clauses
   * to determine their output schema. This is useful for schema inference and validation
   * before processing actual data.
   * </p>
   *
   * @param context the projection context to use for schema determination
   * @return a list of projected column descriptors representing the output schema
   */
  @Nonnull
  public List<ProjectedColumn> getResultSchema(@Nonnull final ProjectionContext context) {
    // Create a stub context to determine the types of the results
    final ProjectionContext stubContext = context.withInputContext(
        context.inputContext().copyWith(DefaultRepresentation.empty()));
    final List<ProjectionResult> stubResults = projectionClauses.stream()
        .map(s -> s.evaluate(stubContext))
        .toList();
    return stubResults.stream()
        .flatMap(sr -> sr.getResults().stream())
        .toList();
  }
}
