/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import org.hl7.fhir.r4.model.Parameters;

/**
 * HAPI plain provider that provides an entry point for the `$aggregate` system-wide operation.
 *
 * @author John Grimes
 */
public class AggregateProvider {

  private final AggregateExecutor aggregateExecutor;

  public AggregateProvider(AggregateExecutor aggregateExecutor) {
    assert aggregateExecutor != null : "Must supply aggregate executor";

    this.aggregateExecutor = aggregateExecutor;
  }

  @Operation(name = "$aggregate", idempotent = true)
  public Parameters aggregate(@ResourceParam Parameters parameters, RequestDetails requestDetails) {
    /* TODO: Remove the passing down of the FHIR server base, if its not needed. */
    AggregateRequest query = new AggregateRequest(parameters, requestDetails.getFhirServerBase());
    AggregateResponse result = aggregateExecutor.execute(query);
    return result.toParameters();
  }

}
