package au.csiro.pathling.update;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResourceWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Common functionality for updating resource datasets with PUT operations
 *
 * @author Sean Fong
 */

@Component
public class UpdateHelpers {
  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final ResourceReader resourceReader;

  @Nonnull
  private final ResourceWriter resourceWriter;

  public UpdateHelpers(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final ResourceWriter resourceWriter) {
    this.spark = spark;
    this.fhirEncoders = fhirEncoders;
    this.resourceReader = resourceReader;
    this.resourceWriter = resourceWriter;
  }

  public void updateDataset(final ResourceType resourceType, final IBaseResource resource) {
    final Encoder<IBaseResource> encoder = fhirEncoders.of(resourceType.toCode());
    final Dataset<IBaseResource> dataset = spark.createDataset(List.of(resource), encoder);

    final String resourceId = resource.getIdElement().toString();
    final Dataset<Row> resources = resourceReader.read(resourceType);
    final Dataset<Row> filtered = resources.filter(resources.col("id").equalTo(resourceId));
    if (filtered.isEmpty()) {
      resourceWriter.append(resourceType, dataset);
    } else {
      final Dataset<Row> remainingResources = resources.except(filtered);
      resourceWriter.write(resourceType, remainingResources);
      resourceWriter.append(resourceType, dataset);
    }
  }

  public void appendDataset(final ResourceType resourceType, final IBaseResource resource) {
    final Encoder<IBaseResource> encoder = fhirEncoders.of(resourceType.toCode());
    final Dataset<IBaseResource> dataset = spark.createDataset(List.of(resource), encoder);

    resourceWriter.append(resourceType, dataset);
  }
}
