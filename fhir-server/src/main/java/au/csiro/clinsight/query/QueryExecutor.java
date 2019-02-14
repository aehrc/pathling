/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import au.csiro.clinsight.resources.AggregateQuery;
import au.csiro.clinsight.resources.AggregateQueryResult;

/**
 * @author John Grimes
 */
public interface QueryExecutor {

  AggregateQueryResult execute(AggregateQuery query);

}
