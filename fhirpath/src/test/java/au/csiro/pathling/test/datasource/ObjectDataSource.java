package au.csiro.pathling.test.datasource;

import static java.util.Objects.requireNonNull;
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
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;

public class ObjectDataSource implements DataSource {

  @Nonnull
  private final Map<String, Dataset<Row>> data = new HashMap<>();

  public ObjectDataSource(@Nonnull final SparkSession spark, @Nonnull final FhirEncoders encoders,
      @Nonnull final List<IBaseResource> resources) {
    final Map<String, List<IBaseResource>> groupedResources = resources.stream()
        .collect(groupingBy(IBase::fhirType));
    groupedResources.forEach((resourceType, resourceList) -> {
      final ExpressionEncoder<IBaseResource> encoder = encoders.of(resourceType);
      final Dataset<Row> dataset = spark.createDataset(resourceList, encoder).toDF();
      data.put(resourceType, dataset);
    });
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final String resourceCode) {
    return requireNonNull(data.get(resourceCode));
  }

  @Override
  public @Nonnull Set<String> getResourceTypes() {
    return data.keySet();
  }

}
