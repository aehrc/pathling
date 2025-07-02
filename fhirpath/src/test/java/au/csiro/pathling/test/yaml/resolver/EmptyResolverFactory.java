package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.def.DefDefinitionContext;
import au.csiro.pathling.fhirpath.definition.def.DefResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceTag;
import au.csiro.pathling.fhirpath.execution.DefResourceResolver;
import au.csiro.pathling.test.yaml.RuntimeContext;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Factory for creating empty resource resolvers. This implementation provides a resolver that
 * returns an empty DataFrame, useful for testing expressions that don't require input data.
 */
@Value
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class EmptyResolverFactory implements Function<RuntimeContext, ResourceResolver> {


  // singleton
  private static final EmptyResolverFactory INSTANCE = new EmptyResolverFactory();

  public static EmptyResolverFactory getInstance() {
    return INSTANCE;
  }

  @Override
  public ResourceResolver apply(final RuntimeContext runtimeContext) {

    final DefResourceTag subjectResourceTag = DefResourceTag.of("Empty");
    return DefResourceResolver.of(
        subjectResourceTag,
        DefDefinitionContext.of(DefResourceDefinition.of(subjectResourceTag)),
        runtimeContext.getSpark().emptyDataFrame()
    );
  }
}
