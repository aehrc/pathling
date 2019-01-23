/**
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use
 * is subject to license terms and conditions.
 */

package au.csiro.clinsight.datasource;

import au.csiro.clinsight.resources.Query;
import au.csiro.clinsight.resources.QueryResult;
import java.sql.SQLException;

public interface QueryExecutor {

  QueryResult execute(Query query)
      throws SQLException, HibernateQueryExecutor.UnsupportedDataTypeException;

}
