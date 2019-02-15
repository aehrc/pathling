/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.fhir.ResourceDefinitions.isSupportedPrimitive;
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

  static void populateJoinsFromElement(ParseResult result, ResolvedElement element) {
    Join previousJoin = null;
    for (MultiValueTraversal multiValueTraversal : element.getMultiValueTraversals()) {
      // Get the components of the path within the traversal.
      LinkedList<String> pathComponents = tokenizePath(multiValueTraversal.getPath());
      // Make the first component all lowercase.
      pathComponents.push(pathComponents.pop().toLowerCase());

      // Construct an alias that can be used to refer to the generated table elsewhere in the query.
      List<String> aliasComponents = pathComponents.subList(1, pathComponents.size());
      List<String> aliasTail = aliasComponents.subList(1, aliasComponents.size()).stream()
          .map(Strings::capitalize).collect(Collectors.toCollection(LinkedList::new));
      String tableAlias = String.join("", aliasComponents.get(0), String.join("", aliasTail));

      // Construct a join expression.
      String udtfExpression = untokenizePath(pathComponents);
      String traversalType = multiValueTraversal.getTypeCode();
      String joinExpression;
      String columnAlias;
      // If the element is primitive, we will need explode. If it is a complex type, we will need
      // inline.
      if (isSupportedPrimitive(traversalType)) {
        columnAlias = pathComponents.getLast();
        joinExpression =
            "LATERAL VIEW OUTER explode(" + udtfExpression + ") " + tableAlias + " AS "
                + columnAlias;
      } else {
        columnAlias = String.join(", ", multiValueTraversal.getChildren());
        joinExpression = "LATERAL VIEW OUTER inline(" + udtfExpression + ") " + tableAlias + " AS "
            + columnAlias;
      }
      Join join = new Join(joinExpression, tableAlias, columnAlias);
      join.setUdtfExpression(udtfExpression);
      join.setTypeCode(traversalType);

      // If this is not the first join, record a dependency between this join and the previous one.
      // The expression needs to be rewritten to refer to the alias of the target join.
      if (previousJoin != null) {
        join.setDependsUpon(previousJoin);
        assert previousJoin.getUdtfExpression() != null;
        String updatedExpression = join.getExpression()
            .replace(previousJoin.getUdtfExpression(), previousJoin.getTableAlias());
        join.setExpression(updatedExpression);
      }

      result.getJoins().add(join);
      previousJoin = join;
    }

    // Rewrite the main expression (SELECT) of the parse result to make use of the table aliases
    // that were created when we processed the joins.
    if (!result.getJoins().isEmpty()) {
      Join finalJoin = result.getJoins().last();
      String updatedExpression;
      assert finalJoin.getUdtfExpression() != null;
      assert finalJoin.getTypeCode() != null;
      if (isSupportedPrimitive(finalJoin.getTypeCode())) {
        updatedExpression = result.getExpression().replace(finalJoin.getUdtfExpression(),
            finalJoin.getTableAlias() + "." + finalJoin.getColumnAlias());
      } else {
        updatedExpression = result.getExpression()
            .replace(finalJoin.getUdtfExpression(), finalJoin.getTableAlias());
      }
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
