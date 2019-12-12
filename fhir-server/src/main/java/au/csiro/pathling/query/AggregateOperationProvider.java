/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query;

import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import org.hl7.fhir.r4.model.Parameters;

/**
 * HAPI plain provider that provides an entry point for the `$aggregate` system-wide operation.
 *
 * @author John Grimes
 */
public class AggregateOperationProvider {

  private final AggregateExecutor aggregateExecutor;

  public AggregateOperationProvider(AggregateExecutor aggregateExecutor) {
    assert aggregateExecutor != null : "Must supply aggregate executor";

    this.aggregateExecutor = aggregateExecutor;
  }

  @Operation(name = "$aggregate", idempotent = true)
  public Parameters aggregate(@ResourceParam Parameters parameters) {
    AggregateRequest query = new AggregateRequest(parameters);
    AggregateResponse result = aggregateExecutor.execute(query);
    return result.toParameters();
  }

}
