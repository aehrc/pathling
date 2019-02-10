/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import java.util.List;
import java.util.Set;
import java.util.SortedSet;

/**
 * @author John Grimes
 */
class QueryPlan {

  private List<String> aggregations;
  private List<String> aggregationTypes;
  private List<String> groupingTypes;
  private List<String> groupings;
  private Set<String> fromTables;
  private SortedSet<Join> joins;

  List<String> getAggregations() {
    return aggregations;
  }

  void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations;
  }

  List<String> getAggregationTypes() {
    return aggregationTypes;
  }

  void setAggregationTypes(List<String> aggregationTypes) {
    this.aggregationTypes = aggregationTypes;
  }

  List<String> getGroupingTypes() {
    return groupingTypes;
  }

  void setGroupingTypes(List<String> groupingTypes) {
    this.groupingTypes = groupingTypes;
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

  SortedSet<Join> getJoins() {
    return joins;
  }

  void setJoins(SortedSet<Join> joins) {
    this.joins = joins;
  }
}
