/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.fhir;

import static au.csiro.clinsight.utilities.Preconditions.checkNotNull;

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
    checkNotNull(queryExecutor, "Must supply query executor");

    this.queryExecutor = queryExecutor;
  }

  @Operation(name = "$query", idempotent = true)
  public AggregateQueryResult queryOperation(@OperationParam(name = "query") AggregateQuery query) {
    return queryExecutor.execute(query);
  }

}
