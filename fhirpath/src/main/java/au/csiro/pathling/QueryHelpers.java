/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling;

import au.csiro.pathling.encoders.FhirEncoders;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Common functionality for executing queries using Spark.
 *
 * @author John Grimes
 */
public abstract class QueryHelpers {

  private QueryHelpers() {
  }

  /**
   * Creates an empty dataset with the schema of the supplied resource type.
   *
   * @param spark a {@link SparkSession}
   * @param fhirEncoders a {@link FhirEncoders} object
   * @param resourceType the {@link ResourceType} that will determine the shape of the empty
   * dataset
   * @return a new {@link Dataset}
   */
  @Nonnull
  public static Dataset<Row> createEmptyDataset(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders, @Nonnull final ResourceType resourceType) {
    final ExpressionEncoder<IBaseResource> encoder = fhirEncoders.of(resourceType.toCode());
    return spark.emptyDataset(encoder).toDF();
  }
}
