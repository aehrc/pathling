/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import java.util.List;
import java.util.Set;
import org.apache.spark.sql.types.DataType;

/**
 * @author John Grimes
 */
public class SparkQueryPlan {

  private List<String> aggregationClause;
  private List<DataType> resultTypes;
  private List<String> groupingClause;
  private Set<String> fromTables;

  public List<String> getAggregationClause() {
    return aggregationClause;
  }

  public void setAggregationClause(List<String> aggregationClause) {
    this.aggregationClause = aggregationClause;
  }

  public List<DataType> getResultTypes() {
    return resultTypes;
  }

  public void setResultTypes(List<DataType> resultTypes) {
    this.resultTypes = resultTypes;
  }

  public List<String> getGroupingClause() {
    return groupingClause;
  }

  public void setGroupingClause(List<String> groupingClause) {
    this.groupingClause = groupingClause;
  }

  public Set<String> getFromTables() {
    return fromTables;
  }

  public void setFromTables(Set<String> fromTables) {
    this.fromTables = fromTables;
  }

}
