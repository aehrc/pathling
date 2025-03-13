package au.csiro.pathling.fhirpath.execution;

import static au.csiro.pathling.fhirpath.FhirPathConstants.Functions;

import au.csiro.pathling.fhir.FhirUtils;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.operator.CombineOperator;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.EvalOperator;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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


  @Nullable
  public static FhirPath asTypeOf(@Nonnull final FhirPath path) {
    return asFunction(path, Functions.OF_TYPE);
  }

  public static FhirPath asResource(@Nonnull final FhirPath path,
      @Nonnull final FhirContext fhirContext) {
    return path instanceof Paths.Resource resource
               && FhirUtils.isKnownResource(resource.getResourceCode(), fhirContext)
           ? resource
           : null;
  }

  public static boolean isExtension(@Nonnull final FhirPath path) {
    return asFunction(path, Functions.EXTENSION) != null;
  }

  public static boolean isIif(@Nonnull final FhirPath path) {
    return asFunction(path, Functions.IIF) != null;
  }

  @Nonnull
  public static FhirPath toTraversal(@Nonnull final FhirPath path) {
    if (path instanceof Paths.Traversal traversal) {
      return traversal;
    } else if (isExtension(path)) {
      // the property name is the same as the function name (extension)
      return new Paths.Traversal(Functions.EXTENSION);
    } else {
      return FhirPath.nullPath();
    }
  }

  public static boolean isCombineOperator(@Nonnull final FhirPath path) {
    return path instanceof EvalOperator evalOperator
        && evalOperator.getOperator() instanceof CombineOperator;
  }

  public static boolean isPropagatesArguments(@Nonnull final FhirPath path) {
    return isCombineOperator(path) || isIif(path);
  }

  public static Stream<FhirPath> getPropagatesArguments(@Nonnull final FhirPath path) {
    if (isCombineOperator(path)) {
      return path.children();
    } else if (isIif(path)) {
      return path.children().skip(1);
    } else {
      throw new IllegalArgumentException("Path does not propagate arguments:" + path);
    }
  }
}
