package au.csiro.pathling.fhirpath.function.registry;

import au.csiro.pathling.fhirpath.function.FhirViewFunctions;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.StandardFunctions;
import au.csiro.pathling.fhirpath.function.TerminologyFunctions;
import au.csiro.pathling.fhirpath.function.UntilFunction;
import au.csiro.pathling.fhirpath.function.WrappedFunction;
import com.google.common.collect.ImmutableMap.Builder;

/**
 * A static registry of FHIRPath function implementations, for use in environments where dependency
 * injection is not available.
 *
 * @author John Grimes
 */
public class StaticFunctionRegistry extends InMemoryFunctionRegistry<NamedFunction> {


  private static final StaticFunctionRegistry INSTANCE = new StaticFunctionRegistry();

  public StaticFunctionRegistry() {
    super(new Builder<String, NamedFunction>()
        .put("until", new UntilFunction())
        .putAll(WrappedFunction.mapOf(StandardFunctions.class))
        .putAll(WrappedFunction.mapOf(TerminologyFunctions.class))
        .putAll(WrappedFunction.mapOf(FhirViewFunctions.class))
        .build());
  }

  public static StaticFunctionRegistry getInstance() {
    return INSTANCE;
  }
}
