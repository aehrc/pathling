/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.query.QueryExecutor;
import au.csiro.clinsight.query.QueryRequest;
import au.csiro.clinsight.query.QueryResponse;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import org.hl7.fhir.dstu3.model.Parameters;

/**
 * HAPI plain provider that provides an entry point for the `$query` system-wide operation.
 *
 * @author John Grimes
 */
class QueryOperationProvider {

  private final QueryExecutor queryExecutor;

  QueryOperationProvider(QueryExecutor queryExecutor) {
    assert queryExecutor != null : "Must supply query executor";

    this.queryExecutor = queryExecutor;
  }

  @Operation(name = "$query", idempotent = true)
  public Parameters queryOperation(@ResourceParam Parameters parameters) {
    QueryRequest query = new QueryRequest(parameters);
    QueryResponse result = queryExecutor.execute(query);
    return result.toParameters();
  }

}
