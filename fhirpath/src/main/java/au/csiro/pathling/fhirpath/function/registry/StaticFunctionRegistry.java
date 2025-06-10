package au.csiro.pathling.fhirpath.function.registry;

import au.csiro.pathling.fhirpath.function.MethodDefinedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.provider.BooleanLogicFunctions;
import au.csiro.pathling.fhirpath.function.provider.ExistenceFunctions;
import au.csiro.pathling.fhirpath.function.provider.FhirFunctions;
import au.csiro.pathling.fhirpath.function.provider.FilteringAndProjectionFunctions;
import au.csiro.pathling.fhirpath.function.provider.JoinKeyFunctions;
import au.csiro.pathling.fhirpath.function.provider.StringFunctions;
import au.csiro.pathling.fhirpath.function.provider.SubsettingFunctions;
import au.csiro.pathling.fhirpath.function.provider.TerminologyFunctions;
import com.google.common.collect.ImmutableMap.Builder;

/**
 * A static registry of FHIRPath function implementations, for use in environments where dependency
 * injection is not available.
 *
 * @author John Grimes
 */
public class StaticFunctionRegistry extends InMemoryFunctionRegistry<NamedFunction> {


  private static final StaticFunctionRegistry INSTANCE = new StaticFunctionRegistry();

  /**
   * Constructs a new instance of the registry, populating it with the standard set of functions.
   */
  public StaticFunctionRegistry() {
    super(new Builder<String, NamedFunction>()
        .putAll(MethodDefinedFunction.mapOf(BooleanLogicFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(ExistenceFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(FhirFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(FilteringAndProjectionFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(JoinKeyFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(StringFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(SubsettingFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(TerminologyFunctions.class))
        .build());
  }

  /**
   * @return The singleton instance of the registry
   */
  public static StaticFunctionRegistry getInstance() {
    return INSTANCE;
  }

}
