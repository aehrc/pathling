package au.csiro.pathling.fhirpath.function.registry;

import au.csiro.pathling.fhirpath.function.BooleanLogicFunctions;
import au.csiro.pathling.fhirpath.function.BoundaryFunctions;
import au.csiro.pathling.fhirpath.function.ConversionFunctions;
import au.csiro.pathling.fhirpath.function.ExistenceFunctions;
import au.csiro.pathling.fhirpath.function.FhirFunctions;
import au.csiro.pathling.fhirpath.function.FilteringAndProjectionFunctions;
import au.csiro.pathling.fhirpath.function.JoinKeyFunctions;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.StringFunctions;
import au.csiro.pathling.fhirpath.function.SubsettingFunctions;
import au.csiro.pathling.fhirpath.function.WrappedFunction;
import au.csiro.pathling.terminology.TerminologyFunctions;
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
        .putAll(WrappedFunction.mapOf(BooleanLogicFunctions.class))
        .putAll(WrappedFunction.mapOf(BoundaryFunctions.class))
        .putAll(WrappedFunction.mapOf(ConversionFunctions.class))
        .putAll(WrappedFunction.mapOf(ExistenceFunctions.class))
        .putAll(WrappedFunction.mapOf(FhirFunctions.class))
        .putAll(WrappedFunction.mapOf(FilteringAndProjectionFunctions.class))
        .putAll(WrappedFunction.mapOf(JoinKeyFunctions.class))
        .putAll(WrappedFunction.mapOf(StringFunctions.class))
        .putAll(WrappedFunction.mapOf(SubsettingFunctions.class))
        .putAll(WrappedFunction.mapOf(TerminologyFunctions.class))
        .build());
  }

  /**
   * @return The singleton instance of the registry
   */
  public static StaticFunctionRegistry getInstance() {
    return INSTANCE;
  }

}
