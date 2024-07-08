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

package au.csiro.pathling.test.benchmark;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.CacheableDatabase;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.jmh.AbstractJmhSpringBootState;
import au.csiro.pathling.search.SearchExecutor;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringParam;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.ActiveProfiles;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Tag("UnitTest")
@Fork(0)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
public class SearchDevBenchmark {

  public static final int PAGE_SIZE = 10;

  @State(Scope.Benchmark)
  @ActiveProfiles("unit-test")
  public static class SearchState extends AbstractJmhSpringBootState {

    @Autowired
    SparkSession spark;

    @Autowired
    TerminologyServiceFactory terminologyServiceFactory;

    @Autowired
    QueryConfiguration configuration;

    @Autowired
    FhirContext fhirContext;

    @Autowired
    IParser jsonParser;

    @Autowired
    FhirEncoders fhirEncoders;

    @Autowired
    Database database;

    @Bean
    @ConditionalOnMissingBean
    public static Database database(@Nonnull final ServerConfiguration configuration,
        @Nonnull final SparkSession spark, @Nonnull final FhirEncoders fhirEncoders) {
      return new CacheableDatabase(configuration.getStorage(), spark, fhirEncoders,
          new ThreadPoolTaskExecutor());
    }

    public List<IBaseResource> execute(@Nonnull final Optional<StringAndListParam> filters) {
      final IBundleProvider executor = new SearchExecutor(configuration, fhirContext, spark,
          database, Optional.of(terminologyServiceFactory), fhirEncoders, ResourceType.ENCOUNTER,
          filters);
      executor.size();
      return executor.getResources(0, PAGE_SIZE);
    }
  }

  @Benchmark
  public void referenceExtension_Benchmark(final Blackhole bh, final SearchState executor) {
    final StringAndListParam filters = new StringAndListParam()
        .addAnd(new StringParam(
            "extension.where(url = 'urn:test:associated-goal').valueReference"
                + ".resolve().ofType(Goal).exists()"));
    bh.consume(executor.execute(Optional.of(filters)));
  }

}
