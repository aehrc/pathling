/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import au.csiro.clinsight.query.parsing.Join;
import java.util.List;
import java.util.SortedSet;

/**
 * A description of a query, along with the tables and joins required, which can be used by the
 * QueryExecutor to retrieve the result.
 *
 * @author John Grimes
 */
class QueryPlan {

  /**
   * A list of the SQL expressions that represent the aggregations within this query.
   */
  private List<String> aggregations;

  /**
   * A list of the FHIR type codes returned by each of the aggregation expressions.
   */
  private List<String> aggregationTypes;

  /**
   * A list of the SQL expressions that represent the groupings within this query.
   */
  private List<String> groupings;

  /**
   * A list of the FHIR type codes returned by each of the grouping expressions.
   */
  private List<String> groupingTypes;

  /**
   * The name of the table that is the original target of any joins required for this query.
   */
  private String fromTable;

  /**
   * An ordered set of joins required for the execution of this query.
   */
  private SortedSet<Join> joins;

  /**
   * A list of the SQL expressions that represent any filters within this query.
   */
  private List<String> filters;

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

  public List<String> getGroupings() {
    return groupings;
  }

  public void setGroupings(List<String> groupings) {
    this.groupings = groupings;
  }

  public List<String> getGroupingTypes() {
    return groupingTypes;
  }

  public void setGroupingTypes(List<String> groupingTypes) {
    this.groupingTypes = groupingTypes;
  }

  public String getFromTable() {
    return fromTable;
  }

  public void setFromTable(String fromTable) {
    this.fromTable = fromTable;
  }

  public SortedSet<Join> getJoins() {
    return joins;
  }

  public void setJoins(SortedSet<Join> joins) {
    this.joins = joins;
  }

  public List<String> getFilters() {
    return filters;
  }

  public void setFilters(List<String> filters) {
    this.filters = filters;
  }
}
