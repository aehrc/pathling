/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import au.csiro.clinsight.fhir.definitions.ElementDefinition;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * This class describes the requirements for a join as inferred by the expression parser and
 * recorded within a ParseResult.
 *
 * @author John Grimes
 */
public class Join implements Comparable<Join> {

  /**
   * A SQL expression that can be used to execute this join in support of a query.
   */
  private String sql;

  /**
   * A categorisation of the type of join.
   */
  private JoinType joinType;

  /**
   * An alias for use in identifying the result of the join within a query.
   */
  private String tableAlias;

  /**
   * The definition of the element that this join is designed to allow access to.
   */
  private ElementDefinition targetElement;

  /**
   * An upstream join that this join depends upon. This is used for ordering joins properly within a
   * query.
   */
  private Join dependsUpon;

  public String getSql() {
    return sql;
  }

  public void setSql(@Nonnull String sql) {
    this.sql = sql;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public void setJoinType(@Nonnull JoinType joinType) {
    this.joinType = joinType;
  }

  public String getTableAlias() {
    return tableAlias;
  }

  public void setTableAlias(@Nonnull String tableAlias) {
    this.tableAlias = tableAlias;
  }

  public ElementDefinition getTargetElement() {
    return targetElement;
  }

  public void setTargetElement(@Nonnull ElementDefinition targetElement) {
    this.targetElement = targetElement;
  }

  public Join getDependsUpon() {
    return dependsUpon;
  }

  public void setDependsUpon(@Nonnull Join dependsUpon) {
    this.dependsUpon = dependsUpon;
  }

  /**
   * A join that is dependent on another join is ordered after that join.
   */
  @Override
  public int compareTo(@Nonnull Join j) {
    if (this.equals(j)) {
      return 0;
    } else if (dependsUpon == null && j.getDependsUpon() == null) {
      return 1;
    } else if (j.getDependsUpon() != null && j.getDependsUpon().equals(this)) {
      return -1;
    } else if (dependsUpon != null && dependsUpon.equals(j)) {
      return 1;
    } else {
      Join cursor = j;
      while (cursor != null && cursor.getDependsUpon() != null) {
        if (cursor.getDependsUpon().equals(this)) {
          return -1;
        }
        cursor = cursor.getDependsUpon();
      }
      return 1;
    }
  }

  /**
   * A join is "equal" to another join if it has the same target element.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Join join = (Join) o;
    return Objects.equals(targetElement, join.targetElement);
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetElement);
  }

  /**
   * A categorisation of the type of join, which is used by the query planner to decide how to
   * translate this into executable instructions.
   */
  public enum JoinType {
    /**
     * LATERAL_VIEW - used to explode out rows from fields with max cardinalities greater than one.
     */
    LATERAL_VIEW,
    /**
     * TABLE_JOIN - a regular left outer join, used to resolve references between different resource
     * types.
     */
    TABLE_JOIN,
    /**
     * INLINE_QUERY - a lateral view that has been wrapped within a subquery, to get around the
     * issue that Spark SQL cannot join from a lateral view.
     */
    INLINE_QUERY,
    /**
     * MEMBERSHIP_JOIN - a left outer join to a table (e.g. a ValueSet expansion) for which a
     * boolean value is required based upon whether the code on the left hand side exists within the
     * set of codes on the right hand side. This will later need to be converted to a subquery
     * within a FROM clause, as it requires two levels of aggregation (one to reduce the multiple
     * codes into a single NULL or NOT NULL value, then a second to convert that to boolean and
     * aggregate on the requested aggregation elements).
     */
    MEMBERSHIP_JOIN
  }

}
