/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import ca.uhn.fhir.model.api.annotation.Description;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * HAPI resource provider that can be instantiated using any resource type to add the FHIRPath
 * search functionality.
 *
 * @author John Grimes
 */
public class SearchProvider<T extends IBaseResource> implements IResourceProvider {

  public static final String QUERY_NAME = "fhirPath";
  public static final String PARAMETER_NAME = "filter";
  private final ExecutorConfiguration configuration;
  private final Class<T> resourceType;

  public SearchProvider(ExecutorConfiguration configuration, Class<T> resourceType) {
    this.configuration = configuration;
    this.resourceType = resourceType;
  }

  @Override
  public Class<? extends IBaseResource> getResourceType() {
    return resourceType;
  }

  @Search(queryName = QUERY_NAME)
  public IBundleProvider search(
      @OptionalParam(name = PARAMETER_NAME)
      @Description(shortDefinition = "Filter resources based on a FHIRPath expression")
          StringAndListParam filters) {
    return new SearchExecutor(configuration, ResourceType.fromCode(resourceType.getSimpleName()),
        filters);
  }

}
