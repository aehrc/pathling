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

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.fhir.FhirServer.resourceTypeFromClass;

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
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
 * @see <a href="https://pathling.csiro.au/docs/libraries/fhirpath-query#aggregate">Aggregate</a>
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
   * @param requestDetails the {@link ServletRequestDetails} containing HAPI inferred info
   * @return {@link Parameters} object representing the result
   */
  @Operation(name = "$aggregate", idempotent = true)
  @AsyncSupported
  @OperationAccess("aggregate")
  public Parameters aggregate(
      @Nullable @OperationParam(name = "aggregation") final List<String> aggregation,
      @Nullable @OperationParam(name = "grouping") final List<String> grouping,
      @Nullable @OperationParam(name = "filter") final List<String> filter,
      @SuppressWarnings("unused") @Nullable final ServletRequestDetails requestDetails) {
    final AggregateRequest query = AggregateRequest.fromUserInput(
        resourceType, Optional.ofNullable(aggregation), Optional.ofNullable(grouping),
        Optional.ofNullable(filter));
    final AggregateResponse result = aggregateExecutor.execute(query);
    return result.toParameters();
  }

}
