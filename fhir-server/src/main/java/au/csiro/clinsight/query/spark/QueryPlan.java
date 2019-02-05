/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import java.util.List;
import java.util.Set;

/**
 * @author John Grimes
 */
class QueryPlan {

  private List<String> aggregations;
  private List<String> aggregationTypes;
  private List<String> groupingTypes;
  private List<String> groupings;
  private Set<String> fromTables;

  public List<String> getAggregations() {
    return aggregations;
  }

  public void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations;
  }

  public List<String> getAggregationTypes() {
    return aggregationTypes;
  }

  public void setAggregationTypes(List<String> aggregationTypes) {
    this.aggregationTypes = aggregationTypes;
  }

  public List<String> getGroupingTypes() {
    return groupingTypes;
  }

  public void setGroupingTypes(List<String> groupingTypes) {
    this.groupingTypes = groupingTypes;
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
