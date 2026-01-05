/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.operations.view;

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

/**
 * A DataSource implementation that creates datasets from a list of FHIR resources.
 *
 * @author John Grimes
 */
public class ObjectDataSource implements DataSource {

  @Nonnull private final Map<String, Dataset<Row>> data = new HashMap<>();

  /**
   * Creates a new ObjectDataSource from a list of FHIR resources.
   *
   * @param spark the Spark session
   * @param encoders the FHIR encoders
   * @param resources the list of FHIR resources
   */
  public ObjectDataSource(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders encoders,
      @Nonnull final List<IBaseResource> resources) {
    final Map<String, List<IBaseResource>> groupedResources =
        resources.stream().collect(groupingBy(IBase::fhirType));
    groupedResources.forEach(
        (resourceType, resourceList) -> {
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
  @Nonnull
  public Set<String> getResourceTypes() {
    return data.keySet();
  }
}
