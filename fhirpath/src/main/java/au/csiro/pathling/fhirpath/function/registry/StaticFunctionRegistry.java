package au.csiro.pathling.fhirpath.function.registry;

import static au.csiro.pathling.fhirpath.function.BooleansTestFunction.BooleansTestType.ALL_FALSE;
import static au.csiro.pathling.fhirpath.function.BooleansTestFunction.BooleansTestType.ALL_TRUE;
import static au.csiro.pathling.fhirpath.function.BooleansTestFunction.BooleansTestType.ANY_FALSE;
import static au.csiro.pathling.fhirpath.function.BooleansTestFunction.BooleansTestType.ANY_TRUE;

import au.csiro.pathling.fhirpath.function.BooleansTestFunction;
import au.csiro.pathling.fhirpath.function.GetIdFunction;
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
        //.put("count", new CountFunction())
        .put("resolve", new ResolveFunction())
        //.put("ofType", new OfTypeFunction())
        .put("reverseResolve", new ReverseResolveFunction())
        .put("memberOf", new MemberOfFunction())
        //.put("where", new WhereFunction())
        .put("subsumes", new SubsumesFunction())
        .put("subsumedBy", new SubsumesFunction(true))
        //.put("empty", new EmptyFunction())
        //.put("first", new FirstFunction())
        //.put("not", new NotFunction())
        .put("iif", new IifFunction())
        .put("translate", new TranslateFunction())
        //.put("sum", new SumFunction())
        .put("anyTrue", new BooleansTestFunction(ANY_TRUE))
        .put("anyFalse", new BooleansTestFunction(ANY_FALSE))
        .put("allTrue", new BooleansTestFunction(ALL_TRUE))
        .put("allFalse", new BooleansTestFunction(ALL_FALSE))
        //.put("extension", new ExtensionFunction())
        .put("until", new UntilFunction())
        //.put("exists", new ExistsFunction())
        .put("display", new DisplayFunction())
        .put("property", new PropertyFunction())
        .put("designation", new DesignationFunction())
        .put("toString", new ToStringFunction())
        .put("getId", new GetIdFunction())
        //.putAll(ColumnFunction0.mapOf(ColumnFunctions.class))
        .putAll(WrappedFunction.mapOf(StandardFunctions.class))
        .build());
  }

  public static StaticFunctionRegistry getInstance() {
    return INSTANCE;
  }

}
