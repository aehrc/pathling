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

package au.csiro.pathling.search;

import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.StringAndListParam;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
@Getter
class SearchExecutorBuilder {

  @Nonnull
  final QueryConfiguration configuration;

  @Nonnull
  final FhirContext fhirContext;

  @Nonnull
  final SparkSession sparkSession;

  @Nonnull
  final FhirEncoders fhirEncoders;

  @Nonnull
  final Database database;

  @Nonnull
  final TerminologyServiceFactory terminologyServiceFactory;

  @Nullable
  ResourceType subjectResource;

  @Nonnull
  Optional<StringAndListParam> filters = Optional.empty();

  SearchExecutorBuilder(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.fhirEncoders = fhirEncoders;
    database = mock(Database.class);
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  SearchExecutorBuilder withSubjectResource(@Nonnull final ResourceType resourceType) {
    this.subjectResource = resourceType;
    TestHelpers.mockResource(database, sparkSession, resourceType);
    return this;
  }

  SearchExecutorBuilder withFilters(@Nonnull final StringAndListParam filters) {
    this.filters = Optional.of(filters);
    return this;
  }

  SearchExecutor build() {
    requireNonNull(subjectResource);
    return new SearchExecutor(configuration, fhirContext, sparkSession, database,
        Optional.of(terminologyServiceFactory), fhirEncoders, subjectResource, filters);
  }

}
