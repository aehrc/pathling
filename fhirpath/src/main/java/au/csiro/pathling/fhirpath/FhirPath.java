package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import com.google.gson.Gson;
import org.jetbrains.annotations.NotNull;

/**
 * A description of how to take one {@link Collection} and transform it into another.
 *
 * @author John Grimes
 */
@FunctionalInterface
public interface FhirPath {

  @NotNull Collection evaluate(@NotNull final Collection input,
      @NotNull final EvaluationContext context);

  default @NotNull String toJson(final @NotNull Gson gson) {
    return "{\"" + this.getClass().getSimpleName() + "\": "
        + gson.toJson(this, this.getClass()) + "}";
  }

}
