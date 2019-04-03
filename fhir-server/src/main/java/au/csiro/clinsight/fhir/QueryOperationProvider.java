/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.query.AggregateQuery;
import au.csiro.clinsight.query.AggregateQueryResult;
import au.csiro.clinsight.query.QueryExecutor;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import org.hl7.fhir.dstu3.model.Parameters;

/**
 * HAPI plain provider that provides an entry point for the `aggregateQuery` system-wide operation.
 *
 * @author John Grimes
 */
class QueryOperationProvider {

  private final QueryExecutor queryExecutor;

  QueryOperationProvider(QueryExecutor queryExecutor) {
    assert queryExecutor != null : "Must supply query executor";

    this.queryExecutor = queryExecutor;
  }

  @SuppressWarnings("unused")
  @Operation(name = "$aggregateQuery", idempotent = true)
  public Parameters queryOperation(@ResourceParam Parameters parameters) {
    AggregateQuery query = new AggregateQuery(parameters);
    AggregateQueryResult result = queryExecutor.execute(query);
    return result.toParameters();
  }

}
