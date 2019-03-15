/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author John Grimes
 */
@SuppressWarnings("WeakerAccess")
class Join implements Comparable<Join> {

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

  public enum JoinType {
    LATERAL_VIEW, TABLE_JOIN, INLINE_QUERY
  }

}
