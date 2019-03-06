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
  private final String tableAlias;

  @Nonnull
  private String expression;

  @Nullable
  private String udtfExpression;

  @Nonnull
  private String columnAlias;

  @Nullable
  private String typeCode;

  @Nullable
  private Join dependsUpon;

  Join(@Nonnull String expression, @Nonnull String tableAlias, @Nonnull String columnAlias) {
    this.expression = expression;
    this.tableAlias = tableAlias;
    this.columnAlias = columnAlias;
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

  @Nullable
  Join getDependsUpon() {
    return dependsUpon;
  }

  void setDependsUpon(@Nullable Join dependsUpon) {
    this.dependsUpon = dependsUpon;
  }

  @Nullable
  String getUdtfExpression() {
    return udtfExpression;
  }

  void setUdtfExpression(@Nullable String udtfExpression) {
    this.udtfExpression = udtfExpression;
  }

  @Nonnull
  public String getColumnAlias() {
    return columnAlias;
  }

  public void setColumnAlias(@Nonnull String columnAlias) {
    this.columnAlias = columnAlias;
  }

  @Nullable
  public String getTypeCode() {
    return typeCode;
  }

  public void setTypeCode(@Nullable String typeCode) {
    this.typeCode = typeCode;
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
    return expression.equals(join.expression) &&
        Objects.equals(dependsUpon, join.dependsUpon);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, dependsUpon);
  }
}
