/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
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
   * A list of the parse results that represent the aggregations within this query.
   */
  private List<ParseResult> aggregations;

  /**
   * A list of the parse results that represent the groupings within this query.
   */
  private List<ParseResult> groupings;

  /**
   * The name of the table that is the original target of any joins required for this query.
   */
  private String fromTable;

  /**
   * An ordered set of joins required for the execution of this query.
   */
  private SortedSet<Join> joins;

  /**
   * A list of the parse results that represent any filters within this query.
   */
  private List<ParseResult> filters;

  public List<ParseResult> getAggregations() {
    return aggregations;
  }

  public void setAggregations(List<ParseResult> aggregations) {
    this.aggregations = aggregations;
  }

  public List<ParseResult> getGroupings() {
    return groupings;
  }

  public void setGroupings(List<ParseResult> groupings) {
    this.groupings = groupings;
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

  public List<ParseResult> getFilters() {
    return filters;
  }

  public void setFilters(List<ParseResult> filters) {
    this.filters = filters;
  }
}
