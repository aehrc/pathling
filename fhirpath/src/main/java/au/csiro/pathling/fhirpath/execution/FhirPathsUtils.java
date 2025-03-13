package au.csiro.pathling.fhirpath.execution;

import static au.csiro.pathling.fhirpath.FhirPathConstants.Functions;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.operator.CombineOperator;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.EvalOperator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;

@UtilityClass
public class FhirPathsUtils {


  @Nullable
  public static FhirPath asFunction(@Nonnull final FhirPath path,
      @Nonnull final String functionName) {
    if (path instanceof EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
        .equals(functionName)) {
      return evalFunction;
    }
    return null;
  }

  @Nullable
  public static FhirPath asReverseResolve(@Nonnull final FhirPath path) {
    return asFunction(path, Functions.REVERSE_RESOLVE);
  }

  @Nullable
  public static FhirPath asResolve(@Nonnull final FhirPath path) {
    return asFunction(path, Functions.RESOLVE);
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

  public static boolean isCombineOperator(@Nonnull final FhirPath path) {
    return path instanceof EvalOperator evalOperator
        && evalOperator.getOperator() instanceof CombineOperator;
  }

  public static boolean isIif(@Nonnull final FhirPath path) {
    return path instanceof EvalFunction evalFunction && evalFunction.getFunctionIdentifier()
        .equals("iif");
  }

  public static boolean isPropagatesArguments(@Nonnull final FhirPath path) {
    return isCombineOperator(path) || isIif(path);
  }

  public static Stream<FhirPath> gePropagatesArguments(@Nonnull final FhirPath path) {
    if (isCombineOperator(path)) {
      return path.children();
    } else if (isIif(path)) {
      return path.children().skip(1);
    } else {
      throw new IllegalArgumentException("Path does not propagate arguments:" + path);
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
