package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.operator.CombineOperator;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.EvalOperator;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;

@UtilityClass
public class FhirPathsUtils {

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

  @Nonnull
  public static FhirPath toTraversal(@Nonnull final FhirPath path) {
    if (path instanceof Paths.Traversal traversal) {
      return traversal;
    } else if (isExtension(path)) {
      return new Paths.Traversal("extension");
    } else {
      return FhirPath.nullPath();
    }
  }

  public static boolean isExtension(@Nonnull final FhirPath path) {
    return path instanceof Paths.EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
        .equals("extension");
  }

  public static boolean isTypeOf(@Nonnull final FhirPath path) {
    return asTypeOf(path).isPresent();
  }

  public static boolean isCombineOperation(@Nonnull final FhirPath path) {
    return path instanceof EvalOperator evalOperator
        && evalOperator.getOperator() instanceof CombineOperator;
  }

  public static boolean isIif(@Nonnull final FhirPath path) {
    return path instanceof EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
        .equals("iif");
  }

  public static boolean isMulitPath(@Nonnull final FhirPath path) {
    return isCombineOperation(path) || isIif(path);
  }

  public static Stream<FhirPath> getHeads(@Nonnull final FhirPath path) {
    if (isCombineOperation(path)) {
      return path.children();
    } else if (isIif(path)) {
      return path.children().skip(1);
    } else {
      throw new IllegalArgumentException("Not a multi-path");
    }
  }

  @Nonnull
  public static Optional<EvalFunction> asTypeOf(@Nonnull final FhirPath path) {
    return path instanceof EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
        .equals("ofType")
           ? Optional.of(evalFunction)
           : Optional.empty();
  }

  public static boolean isResource(@Nonnull final FhirPath path) {
    // TODO: This is not strictly correct
    //  we also should check if the name of the resource is valid  
    return path instanceof Paths.Resource;
  }

}
