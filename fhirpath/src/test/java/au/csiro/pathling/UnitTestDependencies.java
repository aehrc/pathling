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

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.sql.PathlingUdfConfigurer;
import au.csiro.pathling.sql.udf.TerminologyUdfRegistrar;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.stubs.TestTerminologyServiceFactory;
import au.csiro.pathling.views.ConstantDeclarationTypeAdapterFactory;
import au.csiro.pathling.views.StrictStringTypeAdapterFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import io.github.fhnaumann.funcs.UCUMService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

/**
 * @author John Grimes
 */
@Configuration
@Profile("unit-test")
public class UnitTestDependencies {

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static QueryConfiguration queryConfiguration() {
    return QueryConfiguration.builder().build();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static PathlingVersion version() {
    return new PathlingVersion();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static SparkSession sparkSession(
      @SuppressWarnings("unused") @Nonnull final Environment environment,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {

    final SparkSession spark = SparkSession.builder()
        .master("local[1]")
        .appName("pathling-unittest")
        .config("spark.default.parallelism", 1)
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.sql.debug.maxToStringFields", 100)
        .config("spark.network.timeout", "600s")
        .config("spark.ui.enabled", false)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
        .getOrCreate();
    TerminologyUdfRegistrar.registerUdfs(spark, terminologyServiceFactory);
    PathlingUdfConfigurer.registerUDFs(spark);
    return spark;
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  public static FhirContext fhirContext() {
    return FhirContext.forR4();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  public static IParser jsonParser(@Nonnull final FhirContext fhirContext) {
    return fhirContext.newJsonParser();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static FhirEncoders fhirEncoders() {
    return FhirEncoders.forR4()
        .withExtensionsEnabled(true)
        .withAllOpenTypes()
        .getOrCreate();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static TerminologyService terminologyService() {
    return SharedMocks.getOrCreate(TerminologyService.class);
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static TerminologyServiceFactory terminologyClientFactory() {
    return new TestTerminologyServiceFactory();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static UCUMService ucumService() {
    return Ucum.service();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static Gson gson() {
    final GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapterFactory(new ConstantDeclarationTypeAdapterFactory());
    builder.registerTypeAdapterFactory(new StrictStringTypeAdapterFactory());
    return builder.create();
  }

}
