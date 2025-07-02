package au.csiro.pathling.test.yaml;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.yaml.resolver.ResolverBuilder;
import jakarta.annotation.Nonnull;
import java.util.function.Function;
import lombok.Value;
import org.apache.spark.sql.SparkSession;

/**
 * Represents a runtime context for test execution, providing access to Spark session and FHIR
 * encoders.
 */
@Value(staticConstructor = "of")
public class RuntimeContext implements ResolverBuilder {

  @Nonnull
  SparkSession spark;
  @Nonnull
  FhirEncoders fhirEncoders;

  @Override
  @Nonnull
  public ResourceResolver create(
      @Nonnull final Function<RuntimeContext, ResourceResolver> resolveFactory) {
    return resolveFactory.apply(this);
  }
}
