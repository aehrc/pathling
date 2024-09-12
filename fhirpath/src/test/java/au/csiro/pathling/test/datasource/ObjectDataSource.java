package au.csiro.pathling.test.datasource;

import static java.util.stream.Collectors.groupingBy;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.source.DataSource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

public class ObjectDataSource implements DataSource {

  @Nonnull
  private final Map<ResourceType, Dataset<Row>> data = new HashMap<>();

  public ObjectDataSource(@Nonnull final SparkSession spark, @Nonnull final FhirEncoders encoders,
      @Nonnull final List<IBaseResource> resources) {
    final Map<ResourceType, List<IBaseResource>> groupedResources = resources.stream()
        .collect(groupingBy((IBaseResource resource) ->
            ResourceType.fromCode(resource.fhirType())));
    groupedResources.forEach((resourceType, resourceList) -> {
      final ExpressionEncoder<IBaseResource> encoder = encoders.of(resourceType.toCode());
      final Dataset<Row> dataset = spark.createDataset(resourceList, encoder).toDF();
      data.put(resourceType, dataset);
    });
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final ResourceType resourceType) {
    return data.get(resourceType);
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final String resourceCode) {
    return data.get(ResourceType.fromCode(resourceCode));
  }

  @Nonnull
  @Override
  public Set<ResourceType> getResourceTypes() {
    return data.keySet();
  }

}
