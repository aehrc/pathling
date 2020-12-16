/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.search;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.StringAndListParam;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.mockito.Mockito;

/**
 * @author John Grimes
 */
@Getter
public class SearchExecutorBuilder {

  @Nonnull
  private final Configuration configuration;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final ResourceReader resourceReader;

  @Nonnull
  private final TerminologyClient terminologyClient;

  @Nonnull
  private final TerminologyClientFactory terminologyClientFactory;

  @Nullable
  private ResourceType subjectResource;

  @Nonnull
  private Optional<StringAndListParam> filters = Optional.empty();

  public SearchExecutorBuilder(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final FhirEncoders fhirEncoders) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.fhirEncoders = fhirEncoders;
    resourceReader = mock(ResourceReader.class);
    terminologyClient = mock(TerminologyClient.class, Mockito.withSettings().serializable());

    terminologyClientFactory =
        mock(TerminologyClientFactory.class, Mockito.withSettings().serializable());
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);
  }

  public SearchExecutorBuilder withSubjectResource(@Nonnull final ResourceType resourceType) {
    this.subjectResource = resourceType;
    TestHelpers.mockResourceReader(resourceReader, sparkSession, resourceType);
    return this;
  }

  public SearchExecutorBuilder withFilters(@Nonnull final StringAndListParam filters) {
    this.filters = Optional.of(filters);
    return this;
  }

  public SearchExecutor build() {
    checkNotNull(subjectResource);
    return new SearchExecutor(configuration, fhirContext, sparkSession, resourceReader,
        Optional.of(terminologyClient), Optional.of(terminologyClientFactory), fhirEncoders,
        subjectResource, filters);
  }

}
