/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import java.util.List;
import java.util.Set;
import org.apache.spark.sql.types.DataType;

/**
 * @author John Grimes
 */
class QueryPlan {

  private List<String> aggregations;
  private List<DataType> resultTypes;
  private List<String> groupings;
  private Set<String> fromTables;

  List<String> getAggregations() {
    return aggregations;
  }

  void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations;
  }

  List<DataType> getResultTypes() {
    return resultTypes;
  }

  void setResultTypes(List<DataType> resultTypes) {
    this.resultTypes = resultTypes;
  }

  List<String> getGroupings() {
    return groupings;
  }

  void setGroupings(List<String> groupings) {
    this.groupings = groupings;
  }

  Set<String> getFromTables() {
    return fromTables;
  }

  void setFromTables(Set<String> fromTables) {
    this.fromTables = fromTables;
  }

}
