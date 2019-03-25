/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

/**
 * @author John Grimes
 */
public interface QueryExecutor {

  AggregateQueryResult execute(AggregateQuery query);

}
