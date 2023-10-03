package au.csiro.pathling.fhirpath.function.registry;

import au.csiro.pathling.fhirpath.function.IifFunction;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.ResolveFunction;
import au.csiro.pathling.fhirpath.function.ReverseResolveFunction;
import au.csiro.pathling.fhirpath.function.StandardFunctions;
import au.csiro.pathling.fhirpath.function.ToStringFunction;
import au.csiro.pathling.fhirpath.function.UntilFunction;
import au.csiro.pathling.fhirpath.function.WrappedFunction;
import au.csiro.pathling.fhirpath.function.terminology.DesignationFunction;
import au.csiro.pathling.fhirpath.function.terminology.DisplayFunction;
import au.csiro.pathling.fhirpath.function.terminology.FhirViewFunctions;
import au.csiro.pathling.fhirpath.function.terminology.MemberOfFunction;
import au.csiro.pathling.fhirpath.function.terminology.PropertyFunction;
import au.csiro.pathling.fhirpath.function.terminology.SubsumesFunction;
import au.csiro.pathling.fhirpath.function.terminology.TranslateFunction;
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
        .put("resolve", new ResolveFunction())
        .put("reverseResolve", new ReverseResolveFunction())
        .put("memberOf", new MemberOfFunction())
        .put("subsumes", new SubsumesFunction())
        .put("subsumedBy", new SubsumesFunction(true))
        .put("iif", new IifFunction())
        .put("translate", new TranslateFunction())
        .put("until", new UntilFunction())
        .put("display", new DisplayFunction())
        .put("property", new PropertyFunction())
        .put("designation", new DesignationFunction())
        .put("toString", new ToStringFunction())
        .putAll(WrappedFunction.mapOf(StandardFunctions.class))
        .putAll(WrappedFunction.mapOf(FhirViewFunctions.class))
        .build());
  }

  public static StaticFunctionRegistry getInstance() {
    return INSTANCE;
  }

}
