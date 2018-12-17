/**
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.datasource;

import au.csiro.clinsight.persistence.Query;
import au.csiro.clinsight.persistence.QueryResult;

import java.sql.SQLException;

public interface QueryExecutor {

    public QueryResult execute(Query query)
            throws SQLException, HibernateQueryExecutor.UnsupportedDataTypeException;

}
