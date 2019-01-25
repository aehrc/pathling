/**
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use
 * is subject to license terms and conditions.
 */

package au.csiro.clinsight.datasource;

import au.csiro.clinsight.resources.AggregateQuery;
import au.csiro.clinsight.resources.AggregateQueryResult;
import java.sql.SQLException;

public interface QueryExecutor {

  AggregateQueryResult execute(AggregateQuery query)
      throws SQLException, HibernateQueryExecutor.UnsupportedDataTypeException;

}
