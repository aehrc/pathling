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
  private String tableAlias;

  @Nonnull
  private String expression;

  @Nonnull
  private String rootExpression;

  @Nullable
  private String udtfExpression;

  @Nullable
  private String traversalType;

  @Nonnull
  private JoinType joinType;

  @Nullable
  private Join dependsUpon;

  Join(@Nonnull String expression, @Nonnull String rootExpression, @Nonnull JoinType joinType,
      @Nonnull String tableAlias) {
    this.expression = expression;
    this.rootExpression = rootExpression;
    this.joinType = joinType;
    this.tableAlias = tableAlias;
  }

  @Nonnull
  String getExpression() {
    return expression;
  }

  void setExpression(@Nonnull String expression) {
    this.expression = expression;
  }

  @Nonnull
  String getTableAlias() {
    return tableAlias;
  }

  public void setTableAlias(@Nonnull String tableAlias) {
    this.tableAlias = tableAlias;
  }

  @Nullable
  Join getDependsUpon() {
    return dependsUpon;
  }

  void setDependsUpon(@Nullable Join dependsUpon) {
    this.dependsUpon = dependsUpon;
  }

  @Nonnull
  String getRootExpression() {
    return rootExpression;
  }

  void setRootExpression(@Nonnull String rootExpression) {
    this.rootExpression = rootExpression;
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

  @Nonnull
  public JoinType getJoinType() {
    return joinType;
  }

  public void setJoinType(@Nonnull JoinType joinType) {
    this.joinType = joinType;
  }

  /**
   * A join that is dependent on another join is ordered after that join.
   */
  @Override
  public int compareTo(@Nonnull Join j) {
    if (this.equals(j)) {
      return 0;
    } else if (j.getDependsUpon() == this) {
      return -1;
    } else if (dependsUpon == j) {
      return 1;
    } else {
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
    return Objects.hash(expression, dependsUpon);
  }

  public enum JoinType {
    LATERAL_VIEW, TABLE_JOIN
  }

}
