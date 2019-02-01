/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.datasource;

import java.util.HashSet;
import java.util.Set;
import org.apache.spark.sql.types.DataType;

/**
 * @author John Grimes
 */
public class SparkQueryPlan {

  private String aggregationClause;
  private Set<String> fromTables;
  private DataType resultType;

  public SparkQueryPlan() {
    aggregationClause = "";
    fromTables = new HashSet<>();
  }

  public String getAggregationClause() {
    return aggregationClause;
  }

  public void setAggregationClause(String aggregationClause) {
    this.aggregationClause = aggregationClause;
  }

  public Set<String> getFromTables() {
    return fromTables;
  }

  public void setFromTables(Set<String> fromTables) {
    this.fromTables = fromTables;
  }

  public DataType getResultType() {
    return resultType;
  }

  public void setResultType(DataType resultType) {
    this.resultType = resultType;
  }

}
