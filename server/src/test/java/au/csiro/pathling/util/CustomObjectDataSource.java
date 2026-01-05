package au.csiro.pathling.util;

import static java.util.stream.Collectors.groupingBy;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DatasetSource;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * @author Felix Naumann
 */
public class CustomObjectDataSource extends DatasetSource {
  public CustomObjectDataSource(
      @Nonnull final SparkSession spark,
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final FhirEncoders encoders,
      @Nonnull final List<IBaseResource> resources) {
    super(pathlingContext);

    final Map<String, List<IBaseResource>> groupedResources =
        resources.stream().collect(groupingBy(IBase::fhirType));
    groupedResources.forEach(
        (resourceType, resourceList) -> {
          final ExpressionEncoder<IBaseResource> encoder = encoders.of(resourceType);
          final Dataset<Row> dataset = spark.createDataset(resourceList, encoder).toDF();
          dataset(resourceType, dataset);
        });
  }
}
