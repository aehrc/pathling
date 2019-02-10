/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author John Grimes
 */
class Join implements Comparable<Join> {

  @Nonnull
  private final String alias;
  @Nonnull
  private String expression;
  @Nullable
  private Join dependsUpon;
  @Nullable
  private String inlineExpression;

  Join(@Nonnull String expression, @Nonnull String alias) {
    this.expression = expression;
    this.alias = alias;
  }

  @Nonnull
  String getExpression() {
    return expression;
  }

  void setExpression(@Nonnull String expression) {
    this.expression = expression;
  }

  @Nonnull
  String getAlias() {
    return alias;
  }

  @Nullable
  Join getDependsUpon() {
    return dependsUpon;
  }

  void setDependsUpon(@Nullable Join dependsUpon) {
    this.dependsUpon = dependsUpon;
  }

  @Nullable
  String getInlineExpression() {
    return inlineExpression;
  }

  void setInlineExpression(@Nullable String inlineExpression) {
    this.inlineExpression = inlineExpression;
  }

  @Override
  public int compareTo(@Nonnull Join j) {
    if (j.getDependsUpon() == this) {
      return -1;
    } else if (getDependsUpon() == j) {
      return 1;
    } else {
      return 0;
    }
  }
}
