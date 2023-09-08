package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * A description of how to take one {@link Collection} and transform it into another.
 *
 * @param <I> The input type of {@link Collection}
 * @param <O> The output type of {@link Collection}
 * @author John Grimes
 */
@FunctionalInterface
public interface FhirPath<I extends Collection, O extends Collection> {

  O apply(@Nonnull final I input, @Nonnull final EvaluationContext context);

  static Column applyOperation(@Nonnull final EvaluationContext context,
      @Nonnull final Collection input, @Nonnull final Function<Column, Column> singularOperation,
      @Nonnull final Function<Column, Column> collectionOperation) {
    if (input.isSingular(context)) {
      return singularOperation.apply(input.getColumn());
    } else {
      return collectionOperation.apply(input.getColumn());
    }
  }

}
