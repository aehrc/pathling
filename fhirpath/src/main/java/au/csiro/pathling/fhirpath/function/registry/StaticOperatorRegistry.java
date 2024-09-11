package au.csiro.pathling.fhirpath.function.registry;

import au.csiro.pathling.fhirpath.operator.BinaryOperator;
import au.csiro.pathling.fhirpath.operator.comparison.ComparisonOperation;
import au.csiro.pathling.fhirpath.operator.comparison.ComparisonOperator;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.Map;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;

/**
 * An implementation of {@link FunctionRegistry} that stores operator instances in an in-memory map
 * structure.
 *
 * @author John Grimes
 */
public class StaticOperatorRegistry implements FunctionRegistry<BinaryOperator> {

  @NotNull
  private final Map<String, BinaryOperator> functions;

  public StaticOperatorRegistry() {
    this.functions = new Builder<String, BinaryOperator>()
        .put("=", new ComparisonOperator(ComparisonOperation.EQUALS))
        .put("!=", new ComparisonOperator(ComparisonOperation.NOT_EQUALS))
        .put("<", new ComparisonOperator(ComparisonOperation.LESS_THAN))
        .put("<=", new ComparisonOperator(ComparisonOperation.LESS_THAN_OR_EQUAL_TO))
        .put(">", new ComparisonOperator(ComparisonOperation.GREATER_THAN))
        .put(">=", new ComparisonOperator(ComparisonOperation.GREATER_THAN_OR_EQUAL_TO))
        .build();
  }

  @Override
  public Optional<BinaryOperator> getInstance(final String name) {
    return Optional.ofNullable(functions.get(name));
  }

}
