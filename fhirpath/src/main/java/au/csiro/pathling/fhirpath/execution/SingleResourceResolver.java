package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@EqualsAndHashCode(callSuper = true)
@Value
public class SingleResourceResolver extends BaseResourceResolver {

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  FhirContext fhirContext;
  
  @Nonnull
  DataSource dataSource;

  @Override
  @Nonnull
  public Dataset<Row> createView() {
    return resourceDataset(dataSource, subjectResource);
  }
}
