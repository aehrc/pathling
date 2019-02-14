/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.query.QueryExecutor;
import au.csiro.clinsight.resources.AggregateQuery;
import au.csiro.clinsight.resources.AggregateQueryResult;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;

/**
 * @author John Grimes
 */
class QueryOperationProvider {

  private final QueryExecutor queryExecutor;

  QueryOperationProvider(QueryExecutor queryExecutor) {
    assert queryExecutor != null : "Must supply query executor";

    this.queryExecutor = queryExecutor;
  }

  @SuppressWarnings("unused")
  @Operation(name = "$query", idempotent = true)
  public AggregateQueryResult queryOperation(@OperationParam(name = "query") AggregateQuery query) {
    return queryExecutor.execute(query);
  }

}
