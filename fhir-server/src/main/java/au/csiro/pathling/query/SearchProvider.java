/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HAPI resource provider that can be instantiated using any resource type to add the FHIRPath
 * search functionality.
 *
 * @author John Grimes
 */
public class SearchProvider<T extends IBaseResource>
  implements IResourceProvider {
  /* TODO: Test that exception handling works properly in SearchProvider. */

  private static final Logger logger = LoggerFactory.getLogger(
    SearchProvider.class
  );

  public static final String QUERY_NAME = "fhirPath";
  public static final String FILTER_PARAM = "filter";
  private final ExecutorConfiguration configuration;
  private final Class<T> resourceType;

  public SearchProvider(
    ExecutorConfiguration configuration,
    Class<T> resourceType
  ) {
    this.configuration = configuration;
    this.resourceType = resourceType;
  }

  @Override
  public Class<? extends IBaseResource> getResourceType() {
    return resourceType;
  }

  @Search(queryName = QUERY_NAME)
  public IBundleProvider search(
    @OptionalParam(name = FILTER_PARAM) StringAndListParam filters
  ) {
    try {
      return new SearchExecutor(
        configuration,
        ResourceType.fromCode(resourceType.getSimpleName()),
        filters
      );
    } catch (BaseServerResponseException e) {
      // Errors relating to invalid input are re-raised, to be dealt with by HAPI.
      logger.warn("Invalid request", e);
      throw e;
    } catch (Exception | AssertionError e) {
      // All unexpected exceptions get wrapped in a 500 for presenting back to the user.
      throw new InternalErrorException(
        "Unexpected error occurred while executing query",
        e
      );
    }
  }
}
