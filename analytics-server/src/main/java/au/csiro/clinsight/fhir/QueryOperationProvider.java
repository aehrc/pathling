/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.datasource.HibernateQueryExecutor;
import au.csiro.clinsight.datasource.QueryExecutor;
import au.csiro.clinsight.persistence.Query;
import au.csiro.clinsight.persistence.QueryResult;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import java.sql.SQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author John Grimes
 */
@Service
public class QueryOperationProvider {

  private QueryExecutor queryExecutor;

  @Autowired
  public QueryOperationProvider(QueryExecutor queryExecutor) {
    this.queryExecutor = queryExecutor;
  }

  @Operation(name = "$query", idempotent = true)
  public QueryResult queryOperation(@OperationParam(name = "query") Query query)
      throws SQLException, HibernateQueryExecutor.UnsupportedDataTypeException {
    return queryExecutor.execute(query);
  }

}
