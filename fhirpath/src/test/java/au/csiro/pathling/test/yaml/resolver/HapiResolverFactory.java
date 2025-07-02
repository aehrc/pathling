package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.fhirpath.definition.fhir.FhirResourceTag;
import au.csiro.pathling.fhirpath.execution.DefResourceResolver;
import au.csiro.pathling.test.yaml.RuntimeContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Factory for creating resource resolvers from HAPI FHIR resources. This implementation handles the
 * conversion of HAPI FHIR resource objects into a format suitable for FHIRPath expression
 * evaluation.
 */
@Value(staticConstructor = "of")
public class HapiResolverFactory implements Function<RuntimeContext, ResourceResolver> {

  @Nonnull
  IBaseResource resource;

  @Override
  @Nonnull
  public ResourceResolver apply(final RuntimeContext rt) {
    final Dataset<Row> resourceDS = rt.getSpark().createDataset(List.of(resource),
        rt.getFhirEncoders().of(resource.fhirType())).toDF();

    return DefResourceResolver.of(
        FhirResourceTag.of(ResourceType.fromCode(resource.fhirType())),
        FhirDefinitionContext.of(rt.getFhirEncoders().getContext()),
        resourceDS
    );
  }
}
