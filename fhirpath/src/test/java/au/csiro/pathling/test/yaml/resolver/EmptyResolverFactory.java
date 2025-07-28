package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultDefinitionContext;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultResourceDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultResourceTag;
import au.csiro.pathling.fhirpath.execution.DefaultResourceResolver;
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

    final DefaultResourceTag subjectResourceTag = DefaultResourceTag.of("Empty");
    return DefaultResourceResolver.of(
        subjectResourceTag,
        DefaultDefinitionContext.of(DefaultResourceDefinition.of(subjectResourceTag)),
        runtimeContext.getSpark().emptyDataFrame()
    );
  }
}
