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

package au.csiro.pathling.spark;

import au.csiro.pathling.sql.FhirpathUDFRegistrar;
import au.csiro.pathling.sql.udf.TerminologyUdfRegistrar;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import jakarta.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Provides an Apache Spark session for use by the Pathling server.
 *
 * @author John Grimes
 */
@Component
@Profile("core|unit-test")
public class Udfs {

  @Bean
  @Nonnull
  @Qualifier("terminology")
  public static SparkConfigurer terminologyUdfRegistrar(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    return new TerminologyUdfRegistrar(terminologyServiceFactory);
  }

  @Bean
  @Nonnull
  @Qualifier("fhirpath")
  public static SparkConfigurer fhirpathUdfRegistrar() {
    return new FhirpathUDFRegistrar();
  }
}
