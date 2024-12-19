package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import jakarta.annotation.Nonnull;
import lombok.experimental.UtilityClass;
import java.util.Optional;

@UtilityClass
public class PathsUtils {

  public static boolean isReverseResolve(final FhirPath path) {
      return asReverseResolve(path).isPresent();
  }

  public static boolean isResolve(final FhirPath path) {
      return asResolve(path).isPresent();
  }

  @Nonnull
  public static Optional<EvalFunction> asReverseResolve(final FhirPath path) {
      if (path instanceof EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
              .equals("reverseResolve")) {
          return Optional.of(evalFunction);
      }
      return Optional.empty();
  }

  @Nonnull
  public static Optional<EvalFunction> asResolve(final FhirPath path) {
      if (path instanceof EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
              .equals("resolve")) {
          return Optional.of(evalFunction);
      }
      return Optional.empty();
  }

  public static boolean isTraversal(@Nonnull final FhirPath path) {
    return path instanceof Paths.Traversal;
  }

  public static boolean isTypeOf(@Nonnull final FhirPath path) {
    return asTypeOf(path).isPresent();
  }

  @Nonnull
  public static Optional<EvalFunction> asTypeOf(@Nonnull final FhirPath path) {
    return path instanceof EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
        .equals("ofType")
           ? Optional.of(evalFunction)
           : Optional.empty();
  }
}
