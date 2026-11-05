package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Explode, Generator, Inline, PosExplode}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan, Project}

/**
 * Project before generate, for Databricks. The same rewrite as the OSS probe (`PlanRewrite`): every
 * `Generate` of the explode/inline family whose input is not an attribute gets a `Project` directly
 * beneath it that computes the input as a named column. The differences are for DBR's forked
 * Catalyst: the `Generate` is rebuilt positionally with `makeCopy` (by field name, so that a
 * different arity does not matter), and every outcome is counted, so that a silent no-op cannot
 * pass as "no pruning".
 */
object PbgRewrite {
  /** Generates changed by the rewrite. */
  val applied = new AtomicLong
  /** Plans with no Generate to change. */
  val nothingToDo = new AtomicLong
  /** Rewrites that threw (a binary incompatibility, for example). */
  val failed = new AtomicLong
  @volatile var lastFailure: String = ""

  def counters: Seq[Long] = Seq(applied.get, nothingToDo.get, failed.get)

  private def qe(api: Dataset[Row]) = api.asInstanceOf[classic.Dataset[Row]].queryExecution

  private def rewritable(g: Generator): Boolean = g match {
    case _: Inline | _: Explode | _: PosExplode => true
    case _ => false
  }

  private def changes(g: Generate): Boolean =
    rewritable(g.generator) && !g.generator.children.head.isInstanceOf[Attribute]

  /** The analysed plan with a Project under each changed Generate, and how many were changed. */
  def rewritePlan(plan: LogicalPlan): (LogicalPlan, Int) = {
    var n = 0
    val out = plan.transformUp {
      case g: Generate if changes(g) =>
        val input = g.generator.children.head
        val alias = Alias(input, s"nm_$n")()
        n += 1
        val projected = Project(g.child.output :+ alias, g.child)
        val generator = g.generator.withNewChildren(Seq(alias.toAttribute)).asInstanceOf[Generator]
        val unrequired = g.unrequiredChildIndex :+ g.child.output.length
        val args = g.productIterator.map(_.asInstanceOf[AnyRef]).toArray
        var set = 0
        for (i <- args.indices) g.productElementName(i) match {
          case "generator" => args(i) = generator; set += 1
          case "unrequiredChildIndex" => args(i) = unrequired; set += 1
          case "child" => args(i) = projected; set += 1
          case _ => ()
        }
        require(set == 3, s"Generate fields not found by name: ${g.productElementNames.mkString(",")}")
        g.makeCopy(args).asInstanceOf[Generate]
    }
    (out, n)
  }

  /** Runs the rewritten analysed plan of `df`; counts the outcome. Throws if the rewrite fails. */
  def rewrite(df: Dataset[Row]): Dataset[Row] = try {
    val (plan, n) = rewritePlan(qe(df).analyzed)
    if (n == 0) nothingToDo.incrementAndGet() else applied.addAndGet(n)
    classic.Dataset.ofRows(df.sparkSession.asInstanceOf[classic.SparkSession], plan)
  } catch {
    case t: Throwable =>
      failed.incrementAndGet(); lastFailure = t.toString.take(500); throw t
  }

  /** The class of the input and of the child of every Generate in the optimised plan. */
  def optimisedGenerates(df: Dataset[Row]): Seq[String] =
    qe(df).optimizedPlan.collect { case g: Generate =>
      s"${g.generator.children.head.getClass.getSimpleName}<-${g.child.getClass.getSimpleName}"
    }

  def optimisedText(df: Dataset[Row]): String = qe(df).optimizedPlan.treeString
}
