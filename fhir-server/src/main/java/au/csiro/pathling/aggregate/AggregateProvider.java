/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.fhir.FhirServer.resourceTypeFromClass;

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.IResourceProvider;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * HAPI resource provider that provides an entry point for the "aggregate" type-level operation.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/aggregate.html">Aggregate</a>
 */
@Component
@Scope("prototype")
@Profile("server")
public class AggregateProvider implements IResourceProvider {

  @Nonnull
  private final AggregateExecutor aggregateExecutor;

  @Nonnull
  private final Class<? extends IBaseResource> resourceClass;

  @Nonnull
  private final ResourceType resourceType;

  /**
   * @param aggregateExecutor an instance of {@link AggregateExecutor} to process requests
   * @param resourceClass the resource class that this provider will receive requests for
   */
  public AggregateProvider(@Nonnull final AggregateExecutor aggregateExecutor,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.aggregateExecutor = aggregateExecutor;
    this.resourceClass = resourceClass;
    resourceType = resourceTypeFromClass(resourceClass);
  }

  @Override
  public Class<? extends IBaseResource> getResourceType() {
    return resourceClass;
  }

  /**
   * Extended FHIR operation: "aggregate".
   *
   * @param aggregation a list of aggregation expressions
   * @param grouping a list of grouping expressions
   * @param filter a list of filter expressions
   * @param request the {@link HttpServletRequest} details
   * @param requestDetails the {@link RequestDetails} containing HAPI inferred info
   * @param response the {@link HttpServletResponse} response
   * @return {@link Parameters} object representing the result
   */
  @Operation(name = "$aggregate", idempotent = true)
  @AsyncSupported
  public Parameters aggregate(
      @Nullable @OperationParam(name = "aggregation") final List<String> aggregation,
      @Nullable @OperationParam(name = "grouping") final List<String> grouping,
      @Nullable @OperationParam(name = "filter") final List<String> filter,
      @SuppressWarnings("unused") @Nullable final HttpServletRequest request,
      @SuppressWarnings("unused") @Nullable final RequestDetails requestDetails,
      @SuppressWarnings("unused") @Nullable final HttpServletResponse response) {
    return invoke(aggregation, grouping, filter);
  }

  @OperationAccess("aggregate")
  private Parameters invoke(@Nullable final List<String> aggregation,
      @Nullable final List<String> grouping, @Nullable final List<String> filter) {
    final AggregateRequest query = new AggregateRequest(
        resourceType, Optional.ofNullable(aggregation), Optional.ofNullable(grouping),
        Optional.ofNullable(filter));
    final AggregateResponse result = aggregateExecutor.execute(query);
    return result.toParameters();
  }

}
