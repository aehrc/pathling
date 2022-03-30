/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.search;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.StringAndListParam;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
@Getter
class SearchExecutorBuilder {

  @Nonnull
  final Configuration configuration;

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

  SearchExecutorBuilder(@Nonnull final Configuration configuration,
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
    checkNotNull(subjectResource);
    return new SearchExecutor(configuration, fhirContext, sparkSession, database,
        Optional.of(terminologyServiceFactory), fhirEncoders, subjectResource, filters);
  }

}
