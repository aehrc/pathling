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

  private List<String> aggregations;
  private List<DataType> resultTypes;
  private List<String> groupings;
  private Set<String> fromTables;

  public List<String> getAggregations() {
    return aggregations;
  }

  public void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations;
  }

  public List<DataType> getResultTypes() {
    return resultTypes;
  }

  public void setResultTypes(List<DataType> resultTypes) {
    this.resultTypes = resultTypes;
  }

  public List<String> getGroupings() {
    return groupings;
  }

  public void setGroupings(List<String> groupings) {
    this.groupings = groupings;
  }

  public Set<String> getFromTables() {
    return fromTables;
  }

  public void setFromTables(Set<String> fromTables) {
    this.fromTables = fromTables;
  }

}
