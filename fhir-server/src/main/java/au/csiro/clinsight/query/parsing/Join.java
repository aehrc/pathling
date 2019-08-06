/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import static au.csiro.clinsight.query.parsing.Join.JoinType.LATERAL_VIEW;
import static au.csiro.clinsight.query.parsing.Join.JoinType.LEFT_JOIN;

import au.csiro.clinsight.fhir.definitions.ElementDefinition;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * This class describes the requirements for a join as inferred by the expression parser and
 * recorded within a ParseResult.
 *
 * @author John Grimes
 */
public class Join implements Comparable<Join> {

  /**
   * Aliases within SQL expressions are delimited at the front by a non-word character.
   */
  public static final String ALIAS_FRONT_DELIMITER = "(?<=\\b)";

  /**
   * Aliases within SQL expressions are delimited at the rear by a non-word character, followed by
   * an even number of quotes between that and the end of the string (i.e. we don't match anything
   * in quotes).
   *
   * See: https://stackoverflow.com/a/6464500
   */
  public static final String ALIAS_REAR_DELIMITER = "(?=\\b([^']*'[^']*')*[^']*$)";

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
   * The SQL expression that represented the member invocation that necessitated this join, prior to
   * aliasing. This is required so that we can rewrite downstream references to refer to the alias.
   */
  private String aliasTarget;

  /**
   * The definition of the element that this join is designed to allow access to.
   */
  private ElementDefinition targetElement;

  /**
   * An upstream join that this join depends upon. This is used for ordering joins properly within a
   * query.
   */
  private SortedSet<Join> dependsUpon = new TreeSet<>();

  /**
   * Replaces alias targets within a SQL expression with aliases found within the supplied set of
   * joins. Use this for non-join SQL expressions, e.g. SELECT and WHERE.
   */
  public static String rewriteSqlWithJoinAliases(String sql, SortedSet<Join> joins) {
    if (joins.isEmpty()) {
      return sql;
    }
    String newSql = sql;

    // Go through the list and replace the alias target string within the expression with the alias,
    // for each join.
    SortedSet<Join> joinsReversed = new TreeSet<>(Collections.reverseOrder());
    joinsReversed.addAll(joins);
    for (Join currentJoin : joinsReversed) {
      if (currentJoin.getTableAlias() != null && currentJoin.getAliasTarget() != null) {
        newSql = newSql
            .replaceAll(ALIAS_FRONT_DELIMITER + currentJoin.getAliasTarget() + ALIAS_REAR_DELIMITER,
                currentJoin.getTableAlias());
      }
    }

    return newSql;
  }

  /**
   * Reverses the process of replacing alias targets. This is useful where we need to use an
   * expression to modify another expression, e.g. the where function.
   */
  public static String unwindJoinAliases(String sql, SortedSet<Join> joins) {
    if (joins.isEmpty()) {
      return sql;
    }
    String newSql = sql;

    // Go through the list and replace the alias string within the expression with the alias target.
    for (Join currentJoin : joins) {
      if (currentJoin.getTableAlias() != null && currentJoin.getAliasTarget() != null) {
        newSql = newSql
            .replaceAll(ALIAS_FRONT_DELIMITER + currentJoin.getTableAlias() + ALIAS_REAR_DELIMITER,
                currentJoin.getAliasTarget());
      }
    }

    return newSql;
  }

  /**
   * Wrap a set of joins in an inline query and LEFT JOIN to it.
   */
  private static Join wrapViews(SortedSet<Join> joins, String joinAlias, String fromTable) {
    Join lastLateralView = joins.last();
    SortedSet<Join> upstreamDependencies = joins.first().getDependsUpon();
    if (!upstreamDependencies.isEmpty()) {
      assert upstreamDependencies.size() == 1;
      fromTable = upstreamDependencies.first().getAliasTarget();
    }

    String joinExpression =
        "LEFT JOIN (SELECT " + fromTable + ".id, " + lastLateralView.getTableAlias() + ".* FROM "
            + fromTable + " ";
    joinExpression += joins.stream()
        .map(join -> upstreamDependencies.isEmpty()
            ? join.getSql()
            : unwindJoinAliases(join.getSql(), upstreamDependencies))
        .collect(Collectors.joining(" "));

    String joinCondition = fromTable + ".id = " + joinAlias + ".id";
    if (!upstreamDependencies.isEmpty()) {
      joinCondition = rewriteSqlWithJoinAliases(joinCondition, upstreamDependencies);
    }
    joinExpression += ") " + joinAlias + " ON " + joinCondition;

    // Build a new Join object to replace the group of lateral views.
    Join newJoin = new Join();
    newJoin.setSql(joinExpression);
    newJoin.setJoinType(LEFT_JOIN);
    newJoin.setTableAlias(joinAlias + "." + lastLateralView.getTableAlias());
    newJoin.setAliasTarget(lastLateralView.getTableAlias());
    newJoin.setTargetElement(lastLateralView.getTargetElement());

    return newJoin;
  }

  /**
   * Takes a set of views and wraps the set of lateral views that start from the end of this set,
   * and are dependent on one another. This is preparation for appending a left join to the set of
   * joins, which is not allowed directly against a lateral view in Spark SQL.
   */
  public static SortedSet<Join> wrapLateralViews(SortedSet<Join> joins, Join subject,
      AliasGenerator aliasGenerator, String fromTable) {
    SortedSet<Join> result = new TreeSet<>(joins);
    List<SortedSet<Join>> dependencySets = subject
        .getDependencySets(join -> join.getJoinType() == LATERAL_VIEW);
    subject.getDependsUpon().clear();

    for (SortedSet<Join> joinsToWrap : dependencySets) {
      Join wrappedViews = wrapViews(joinsToWrap, aliasGenerator.getAlias(), fromTable);
      subject.getDependsUpon().add(wrappedViews);
      result.removeAll(joinsToWrap);
      result.add(wrappedViews);
    }

    // Rewrite the SQL expression within the subject join to take account of the new aliases.
    subject.setSql(rewriteSqlWithJoinAliases(subject.getSql(), subject.getDependsUpon()));

    return result;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public void setJoinType(JoinType joinType) {
    this.joinType = joinType;
  }

  public String getTableAlias() {
    return tableAlias;
  }

  public void setTableAlias(String tableAlias) {
    this.tableAlias = tableAlias;
  }

  public String getAliasTarget() {
    return aliasTarget;
  }

  public void setAliasTarget(String aliasTarget) {
    this.aliasTarget = aliasTarget;
  }

  public ElementDefinition getTargetElement() {
    return targetElement;
  }

  public void setTargetElement(ElementDefinition targetElement) {
    this.targetElement = targetElement;
  }

  public SortedSet<Join> getDependsUpon() {
    return dependsUpon;
  }

  public void setDependsUpon(SortedSet<Join> dependsUpon) {
    this.dependsUpon = dependsUpon;
  }

  /**
   * A join that is dependent on another join is ordered after that join. If there are no
   * dependencies between the two joins, order lateral views last.
   */
  @Override
  public int compareTo(@Nonnull Join j) {
    // If the two joins are equal according to the `equals` function (same SQL expression), return
    // 0. This will result in the join not being added to a set, for example.
    if (this.equals(j)) {
      return 0;
    }

    // If neither join has a dependency, look at the join type. If the second join is a lateral
    // view, return "less than".
    if (dependsUpon == null && j.getDependsUpon() == null) {
      return j.getJoinType() == LATERAL_VIEW ? -1 : 1;
    }

    // Walk the dependencies upwards from the second join. If it is dependent on the first join,
    // return "less than".
    if (j.isDependentOn(this)) {
      return -1;
    }

    // Walk the dependencies upwards from the first join. If it is dependent on the second join,
    // return "greater than".
    if (isDependentOn(j)) {
      return 1;
    }

    // If neither join is transitively dependent on the other, look at the join type. If the second
    // join is a lateral view, return "less than".
    return j.getJoinType() == LATERAL_VIEW ? -1 : 1;
  }

  /**
   * Recursively finds a join within this join's dependencies.
   */
  private boolean isDependentOn(Join target) {
    for (Join dependency : dependsUpon) {
      if (dependency.equals(target)) {
        return true;
      } else if (dependency.isDependentOn(target)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a set for each join this join depends upon that matches the predicate. Each set will
   * contain the contiguous upstream dependencies from that point which match the predicate.
   */
  private List<SortedSet<Join>> getDependencySets(Predicate<Join> predicate) {
    List<SortedSet<Join>> result = new ArrayList<>();
    for (Join dependency : dependsUpon) {
      if (predicate.test(dependency)) {
        SortedSet<Join> dependencySet = dependency.getAllDependencies(predicate);
        dependencySet.add(dependency);
        result.add(dependencySet);
      }
    }
    return result;
  }

  /**
   * Recursively retrieves all upstream dependencies for this join. The predicate limits both the
   * dependencies that are returned, and those that will be traversed.
   */
  private SortedSet<Join> getAllDependencies(Predicate<Join> predicate) {
    SortedSet<Join> result = new TreeSet<>();
    for (Join dependency : dependsUpon) {
      if (predicate.test(dependency)) {
        result.add(dependency);
        result.addAll(dependency.getAllDependencies(predicate));
      }
    }
    return result;
  }

  /**
   * Two joins are equal if they have the same SQL expression.
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
    return Objects.equals(sql, join.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql);
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
     * LEFT_JOIN - a regular left outer join.
     */
    LEFT_JOIN
  }

}
