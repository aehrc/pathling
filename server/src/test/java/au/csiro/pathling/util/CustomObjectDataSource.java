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
  @SuppressWarnings("this-escape")
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
