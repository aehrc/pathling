/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.datasource;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * A data source over resources built as HAPI objects, in the layout the {@link TestLayout}
 * dimension selects (T036).
 */
public class ObjectDataSource implements DataSource {

  @Nonnull private final Map<String, Dataset<Row>> data = new HashMap<>();

  /**
   * Creates a data source over resources, in the active test layout.
   *
   * @param spark the Spark session
   * @param encoders the encoders for the previous layout, which also carry the FHIR context
   * @param resources the resources, of any mix of types
   */
  public ObjectDataSource(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders encoders,
      @Nonnull final List<IBaseResource> resources) {
    this(spark, encoders, resources, TestLayout.active());
  }

  /**
   * Creates a data source over resources, in a chosen layout.
   *
   * @param spark the Spark session
   * @param encoders the encoders for the previous layout, which also carry the FHIR context
   * @param resources the resources, of any mix of types
   * @param layout the layout to hold the resources in
   */
  public ObjectDataSource(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders encoders,
      @Nonnull final List<IBaseResource> resources,
      @Nonnull final TestLayout layout) {
    final Map<String, List<IBaseResource>> groupedResources =
        resources.stream().collect(groupingBy(IBase::fhirType));
    groupedResources.forEach(
        (resourceType, resourceList) -> {
          final Dataset<Row> dataset =
              LayoutDatasets.fromResources(spark, encoders, layout, resourceType, resourceList);
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
