package au.csiro.pathling.update;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResourceWriter;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Common functionality for updating resource datasets with PUT operations
 *
 * @author Sean Fong
 */

@Component
@Profile("server")
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

  public void appendDataset(final ResourceType resourceType, final IBaseResource resource) {
    appendDataset(resourceType, List.of(resource));
  }

  public void appendDataset(final ResourceType resourceType, final List<IBaseResource> resources) {
    final Encoder<IBaseResource> encoder = fhirEncoders.of(resourceType.toCode());
    final Dataset<Row> dataset = spark.createDataset(resources, encoder).toDF();

    resourceWriter.append(resourceType, dataset);
  }

  public void updateDataset(final ResourceType resourceType, final IBaseResource resource) {
    updateDataset(resourceType, List.of(resource));
  }

  public void updateDataset(final ResourceType resourceType, final List<IBaseResource> resources) {
    final Encoder<IBaseResource> encoder = fhirEncoders.of(resourceType.toCode());
    final Dataset<Row> dataset = spark.createDataset(resources, encoder).toDF();

    resourceWriter.update(resourceType, resourceReader, dataset);
  }
}
