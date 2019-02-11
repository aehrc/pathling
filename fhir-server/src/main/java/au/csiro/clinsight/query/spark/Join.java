/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.utilities.Strings.tokenizePath;
import static au.csiro.clinsight.utilities.Strings.untokenizePath;

import au.csiro.clinsight.fhir.ElementResolver.MultiValueTraversal;
import au.csiro.clinsight.fhir.ElementResolver.ResolvedElement;
import au.csiro.clinsight.utilities.Strings;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
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

  static void populateJoinsFromElement(ParseResult result, ResolvedElement element) {
    Join previousJoin = null;
    for (MultiValueTraversal multiValueTraversal : element.getMultiValueTraversals()) {
      LinkedList<String> pathComponents = tokenizePath(multiValueTraversal.getPath());
      pathComponents.push(pathComponents.pop().toLowerCase());
      List<String> aliasComponents = pathComponents.subList(1, pathComponents.size());
      List<String> aliasTail = aliasComponents.subList(1, aliasComponents.size()).stream()
          .map(Strings::capitalize).collect(Collectors.toCollection(LinkedList::new));
      String alias = String.join("", aliasComponents.get(0), String.join("", aliasTail));
      String inlineExpression = untokenizePath(pathComponents);
      String joinExpression =
          "LATERAL VIEW OUTER inline(" + inlineExpression + ") " + alias + " AS " + String
              .join(", ", multiValueTraversal.getChildren());
      Join join = new Join(joinExpression, alias);
      if (previousJoin != null) {
        join.setDependsUpon(previousJoin);
        assert previousJoin.getInlineExpression() != null;
        String updatedExpression = join.getExpression()
            .replace(previousJoin.getInlineExpression(), previousJoin.getAlias());
        join.setExpression(updatedExpression);
      }
      join.setInlineExpression(inlineExpression);
      result.getJoins().add(join);
      previousJoin = join;
    }
    if (!result.getJoins().isEmpty()) {
      Join finalJoin = result.getJoins().last();
      assert finalJoin.getInlineExpression() != null;
      String updatedExpression = result.getExpression()
          .replace(finalJoin.getInlineExpression(), finalJoin.getAlias());
      result.setExpression(updatedExpression);
    }
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
