/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.search;

import static au.csiro.pathling.fhir.FhirServer.resourceTypeFromClass;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.security.OperationAccess;
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
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * HAPI resource provider that can be instantiated using any resource type to add the FHIRPath
 * search functionality.
 *
 * @author John Grimes
 */
@Component
@Scope("prototype")
@Profile("server")
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
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final Class<? extends IBaseResource> resourceClass;

  @Nonnull
  private final ResourceType resourceType;

  /**
   * @param configuration A {@link Configuration} object to control the behaviour of the executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param resourceReader A {@link ResourceReader} for retrieving resources
   * @param terminologyServiceFactory A {@link TerminologyServiceFactory} for resolving terminology
   * queries within parallel processing
   * @param fhirEncoders A {@link FhirEncoders} object for converting data back into HAPI FHIR
   * objects
   * @param resourceClass A Class that extends {@link IBaseResource} that represents the type of
   * resource to be searched
   */
  public SearchProvider(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.resourceReader = resourceReader;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.fhirEncoders = fhirEncoders;
    this.resourceClass = resourceClass;
    resourceType = resourceTypeFromClass(resourceClass);
  }

  @Override
  @Nonnull
  public Class<? extends IBaseResource> getResourceType() {
    return resourceClass;
  }

  /**
   * Handles all search requests for the resource of the nominated type with no filters.
   *
   * @return A {@link SearchExecutor} which will generate the {@link org.hl7.fhir.r4.model.Bundle}
   * of results
   */
  @Search
  @OperationAccess("search")
  @SuppressWarnings({"UnusedReturnValue"})
  public IBundleProvider search() {
    final ResourceType subjectResource = ResourceDefinition.getResourceTypeFromClass(resourceClass);
    return new CachingSearchExecutor(configuration, fhirContext, sparkSession, resourceReader,
        terminologyServiceFactory, fhirEncoders,
        subjectResource, Optional.empty());
  }

  /**
   * Handles all search requests for the resource of the nominated type that contain filters.
   *
   * @param filters The AND/OR search parameters passed using the "filter" key
   * @return A {@link SearchExecutor} which will generate the {@link org.hl7.fhir.r4.model.Bundle}
   * of results
   */
  @Search(queryName = QUERY_NAME)
  @OperationAccess("search")
  @SuppressWarnings({"UnusedReturnValue"})
  public IBundleProvider search(
      @Nullable @OptionalParam(name = FILTER_PARAM) final StringAndListParam filters) {
    return new CachingSearchExecutor(configuration, fhirContext, sparkSession, resourceReader,
        terminologyServiceFactory, fhirEncoders,
        resourceType, Optional.ofNullable(filters));
  }
}
