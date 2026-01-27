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

package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.function.Function;
import lombok.Value;
import org.apache.spark.sql.SparkSession;

/**
 * Represents a runtime context for test execution, providing access to Spark session and FHIR
 * encoders.
 */
@Value(staticConstructor = "of")
public class RuntimeContext implements ResolverBuilder {

  @Nonnull SparkSession spark;

  @Nonnull FhirEncoders fhirEncoders;

  /**
   * Returns the FHIR context from the FHIR encoders.
   *
   * @return the FHIR context
   */
  @Nonnull
  public FhirContext getFhirContext() {
    return fhirEncoders.getContext();
  }

  @Override
  @Nonnull
  public DatasetEvaluator create(
      @Nonnull final Function<RuntimeContext, DatasetEvaluator> evaluatorFactory) {
    return evaluatorFactory.apply(this);
  }
}
