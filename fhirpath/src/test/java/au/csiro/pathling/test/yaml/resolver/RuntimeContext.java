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

package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
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

  @Nonnull
  SparkSession spark;
  @Nonnull
  FhirEncoders fhirEncoders;

  @Override
  @Nonnull
  public ResourceResolver create(
      @Nonnull final Function<RuntimeContext, ResourceResolver> resolveFactory) {
    return resolveFactory.apply(this);
  }
}
