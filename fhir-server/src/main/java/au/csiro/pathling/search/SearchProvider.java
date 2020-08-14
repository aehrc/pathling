/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.search;

import static au.csiro.pathling.errors.ErrorHandling.handleError;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * HAPI resource provider that can be instantiated using any resource type to add the FHIRPath
 * search functionality.
 *
 * @author John Grimes
 */
@Slf4j
public class SearchProvider implements IResourceProvider {

  /**
   * The name of the FHIR search profile.
   */
  public static final String QUERY_NAME = "fhirPath";

  /**
   * The name of the parameter which is passed to the search profile.
   */
  public static final String FILTER_PARAM = "filter";

  @Nonnull
  private final Configuration configuration;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final ResourceReader resourceReader;

  @Nonnull
  private final Optional<TerminologyClient> terminologyClient;

  @Nonnull
  private final Optional<TerminologyClientFactory> terminologyClientFactory;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final Class<? extends IBaseResource> resourceClass;

  /**
   * @param configuration A {@link Configuration} object to control the behaviour of the executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param resourceReader A {@link ResourceReader} for retrieving resources
   * @param terminologyClient A {@link TerminologyClient} for resolving terminology queries
   * @param terminologyClientFactory A {@link TerminologyClientFactory} for resolving terminology
   * queries within parallel processing
   * @param fhirEncoders A {@link FhirEncoders} object for converting data back into HAPI FHIR
   * objects
   * @param resourceClass A Class that extends {@link IBaseResource} that represents the type of
   * resource to be searched
   */
  public SearchProvider(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyClient> terminologyClient,
      @Nonnull final Optional<TerminologyClientFactory> terminologyClientFactory,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.resourceReader = resourceReader;
    this.terminologyClient = terminologyClient;
    this.terminologyClientFactory = terminologyClientFactory;
    this.fhirEncoders = fhirEncoders;
    this.resourceClass = resourceClass;
  }

  @Override
  @Nonnull
  public Class<? extends IBaseResource> getResourceType() {
    return resourceClass;
  }

  /**
   * Handles all search requests for the resource of the nominated type.
   *
   * @param filters The AND/OR search parameters passed using the "filter" key
   * @return A {@link SearchExecutor} which will generate the {@link org.hl7.fhir.r4.model.Bundle}
   * of results
   */
  @Search(queryName = QUERY_NAME)
  @SuppressWarnings({"UnusedReturnValue", "unused"})
  public IBundleProvider search(
      @Nullable @OptionalParam(name = FILTER_PARAM) final StringAndListParam filters) {
    try {
      final ResourceType subjectResource = ResourceType.fromCode(resourceClass.getSimpleName());
      return new CachingSearchExecutor(configuration, fhirContext, sparkSession, resourceReader,
          terminologyClient, terminologyClientFactory, fhirEncoders,
          subjectResource, Optional.ofNullable(filters));

    } catch (final Throwable e) {
      throw handleError(e);
    }
  }
}
