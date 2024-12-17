package au.csiro.pathling.fhirpath.path;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.Value;
import lombok.experimental.UtilityClass;

@UtilityClass
public class JoinPaths {

  @Value
  public static class ResolvePath implements FhirPath {

    @Nonnull
    public String getTag() {
      return Long.toHexString(System.identityHashCode(this));
    }

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      throw new UnsupportedOperationException("ResolvePath cannot be evaluated directly");
    }

    @Nonnull
    @Override
    public String toExpression() {
      return "resolve()";
    }

    @Nonnull
    public static ResolvePath of(@Nonnull final List<FhirPath> args) {
      return new ResolvePath();
    }

  }

}
