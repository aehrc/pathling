/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class describes the requirements for a join as inferred by the expression parser and
 * recorded within a ParseResult.
 *
 * @author John Grimes
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class Join implements Comparable<Join> {

  @Nonnull
  private final String rootExpression;

  @Nonnull
  private String expression;

  @Nonnull
  private JoinType joinType;

  @Nonnull
  private String tableAlias;

  @Nullable
  private String udtfExpression;

  @Nullable
  private String traversalType;

  @Nullable
  private Join dependsUpon;

  public Join(@Nonnull String expression, @Nonnull String rootExpression,
      @Nonnull JoinType joinType, @Nonnull String tableAlias) {
    this.expression = expression;
    this.rootExpression = rootExpression;
    this.joinType = joinType;
    this.tableAlias = tableAlias;
  }

  @Nonnull
  public String getExpression() {
    return expression;
  }

  public void setExpression(@Nonnull String expression) {
    this.expression = expression;
  }

  @Nonnull
  public String getRootExpression() {
    return rootExpression;
  }

  @Nonnull
  public JoinType getJoinType() {
    return joinType;
  }

  public void setJoinType(@Nonnull JoinType joinType) {
    this.joinType = joinType;
  }

  @Nonnull
  public String getTableAlias() {
    return tableAlias;
  }

  public void setTableAlias(@Nonnull String tableAlias) {
    this.tableAlias = tableAlias;
  }

  @Nullable
  public String getUdtfExpression() {
    return udtfExpression;
  }

  public void setUdtfExpression(@Nullable String udtfExpression) {
    this.udtfExpression = udtfExpression;
  }

  @Nullable
  public String getTraversalType() {
    return traversalType;
  }

  public void setTraversalType(@Nullable String traversalType) {
    this.traversalType = traversalType;
  }

  @Nullable
  public Join getDependsUpon() {
    return dependsUpon;
  }

  public void setDependsUpon(@Nullable Join dependsUpon) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Join join = (Join) o;
    return rootExpression.equals(join.rootExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rootExpression);
  }

  /**
   * A categorisation of the type of join, which is used by the query planner to decide how to
   * translate this into executable instructions.
   *
   * LATERAL_VIEW - used to explode out rows from fields with max cardinalities greater than one.
   *
   * TABLE_JOIN - a regular left outer join, used to resolve references between different resource
   * types.
   *
   * INLINE_QUERY - a lateral view that has been wrapped within a subquery, to get around the issue
   * that Spark SQL cannot join from a lateral view.
   *
   * EXISTS_JOIN - a left outer join to a table (e.g. a ValueSet expansion) for which a boolean
   * value is required based upon whether the code on the left hand side exists within the set of
   * codes on the right hand side. This will later need to be converted to a subquery within a FROM
   * clause, as it requires two levels of aggregation (one to reduce the multiple codes into a
   * single NULL or NOT NULL value, then a second to convert that to boolean and aggregate on the
   * requested aggregation elements).
   */
  public enum JoinType {
    LATERAL_VIEW, TABLE_JOIN, INLINE_QUERY, EXISTS_JOIN
  }

}
