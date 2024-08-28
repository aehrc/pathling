package au.csiro.pathling.fhirpath.function.registry;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.MethodDefinedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.provider.BooleanLogicFunctions;
import au.csiro.pathling.fhirpath.function.provider.ExistenceFunctions;
import au.csiro.pathling.fhirpath.function.provider.FhirFunctions;
import au.csiro.pathling.fhirpath.function.provider.FilteringAndProjectionFunctions;
import au.csiro.pathling.fhirpath.function.provider.JoinKeyFunctions;
import au.csiro.pathling.fhirpath.function.provider.SubsettingFunctions;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.Map;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;

/**
 * An implementation of {@link FunctionRegistry} that stores function instances in an in-memory map
 * structure.
 *
 * @author John Grimes
 */
public class StaticFunctionRegistry implements
    FunctionRegistry<NamedFunction<? extends Collection>> {

  @NotNull
  private final Map<String, NamedFunction<? extends Collection>> functions;

  public StaticFunctionRegistry() {
    this.functions = new Builder<String, NamedFunction<? extends Collection>>()
        .putAll(MethodDefinedFunction.mapOf(BooleanLogicFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(ExistenceFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(FhirFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(FilteringAndProjectionFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(JoinKeyFunctions.class))
        .putAll(MethodDefinedFunction.mapOf(SubsettingFunctions.class))
        .build();
  }

  @Override
  public Optional<NamedFunction<? extends Collection>> getInstance(final String name) {
    return Optional.ofNullable(functions.get(name));
  }

}
