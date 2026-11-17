package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Explode, Generator, Inline, PosExplode}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan, Project}

/**
 * Emulates "project before generate": every `Generate` of the explode/inline family whose
 * generator input is not already an attribute gets a `Project` directly beneath it that computes
 * the input as a named column, and the generator then reads that column. Lives in the
 * `org.apache.spark.sql` package to reach the package-private `Dataset.ofRows`.
 */
object PlanRewrite {

  private def qe(api: Dataset[Row]) = api.asInstanceOf[classic.Dataset[Row]].queryExecution

  private def rewritable(g: Generator): Boolean = g match {
    case _: Inline | _: Explode | _: PosExplode => true
    case _ => false
  }

  /** Rewrites an analysed plan. Each Project sits directly under its own Generate. */
  def rewritePlan(plan: LogicalPlan): LogicalPlan = {
    var n = 0
    plan.transformUp {
      case g: Generate if rewritable(g.generator) &&
          !g.generator.children.head.isInstanceOf[Attribute] =>
        val input = g.generator.children.head
        val alias = Alias(input, s"nm_$n")()
        n += 1
        val projected = Project(g.child.output :+ alias, g.child)
        val generator = g.generator.withNewChildren(Seq(alias.toAttribute)).asInstanceOf[Generator]
        g.copy(generator = generator,
          unrequiredChildIndex = g.unrequiredChildIndex :+ g.child.output.length,
          child = projected)
    }
  }

  /** The number of Generates the rewrite changes. */
  def countRewritten(api: Dataset[Row]): Int =
    qe(api).analyzed.collect { case g: Generate if rewritable(g.generator) &&
      !g.generator.children.head.isInstanceOf[Attribute] => g }.size

  /** Runs the rewritten analysed plan of `df`. */
  def rewrite(api: Dataset[Row]): Dataset[Row] =
    classic.Dataset.ofRows(
      api.sparkSession.asInstanceOf[classic.SparkSession], rewritePlan(qe(api).analyzed))

  /** For each Generate in the optimised plan: its input expression class and its child class. */
  def optimisedGenerates(api: Dataset[Row]): String =
    qe(api).optimizedPlan.collect {
      case g: Generate =>
        s"${g.generator.children.head.getClass.getSimpleName}<-${g.child.getClass.getSimpleName}"
    }.mkString("; ")

  def analysedPlan(api: Dataset[Row]): String = qe(api).analyzed.treeString
  def rewrittenPlan(api: Dataset[Row]): String = rewritePlan(qe(api).analyzed).treeString
  def optimised(api: Dataset[Row]): String = qe(api).optimizedPlan.treeString
}
