/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.datasource.HibernateQueryExecutor;
import au.csiro.clinsight.datasource.QueryExecutor;
import au.csiro.clinsight.resources.AggregateQuery;
import au.csiro.clinsight.resources.AggregateQueryResult;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import java.sql.SQLException;

/**
 * @author John Grimes
 */
public class QueryOperationProvider {

  private QueryExecutor queryExecutor;

  public QueryOperationProvider(QueryExecutor queryExecutor) {
    this.queryExecutor = queryExecutor;
  }

  @Operation(name = "$query", idempotent = true)
  public AggregateQueryResult queryOperation(@OperationParam(name = "query") AggregateQuery query)
      throws SQLException, HibernateQueryExecutor.UnsupportedDataTypeException {
    return queryExecutor.execute(query);
  }

}
