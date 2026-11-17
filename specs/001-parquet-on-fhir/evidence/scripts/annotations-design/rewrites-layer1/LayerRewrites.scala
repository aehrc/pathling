package org.apache.spark.sql

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._

/**
 * Emulates, as rewrites of the analysed plan, the engine changes that the compute-first spike
 * left open. Nothing in `*src` is edited: the engine builds its plan as today and the rewrites
 * change the expressions in it.
 *
 *  - E1: `size(filter(arr, ...))` and `isnull(filter(arr, ...))` on an array of structs (what
 *    `exists()`, `count()` and `empty()` build) read one scalar leaf of the element instead of
 *    the element: `size(filter(arr, x -> x is not null).leaf)`.
 *  - E2: `exists(c)` / `where(c).exists()`, which the engine builds as `size(filter(G, c)) = 0`,
 *    becomes `coalesce(array_max(transform(G, x -> coalesce(c(x), false))), false)` (negated).
 *  - E3: `all(c)`, built as `count(filter(G, c)) = count(G)`, becomes
 *    `coalesce(array_min(transform(G, x -> coalesce(c(x), false))), true)`.
 *  - L1: the dateTime element `struct(value, range)` gets a null `range` when no expression in
 *    the plan reads `range`. The real change would omit the field; a null of the same type
 *    keeps every other expression's types valid and has the same effect on what is read.
 */
object LayerRewrites {

  val counts: mutable.LinkedHashMap[String, Int] = mutable.LinkedHashMap[String, Int]()
  private def bump(k: String): Unit = counts(k) = counts.getOrElse(k, 0) + 1
  def reset(): Unit = counts.clear()
  def summary(): String = if (counts.isEmpty) "none" else counts.map { case (k, v) => s"$k=$v" }.mkString(" ")

  private def qe(api: Dataset[Row]) = api.asInstanceOf[classic.Dataset[Row]].queryExecution

  /** The deterministic leaf: the first atomic field of the element, in schema order. */
  private def leafOf(arr: Expression): Option[Expression] = arr.dataType match {
    case ArrayType(st: StructType, containsNull) =>
      st.fields.zipWithIndex.find { case (f, _) => f.dataType.isInstanceOf[AtomicType] }
        .map { case (f, i) => GetArrayStructFields(arr, f, i, st.fields.length, containsNull) }
    case _ => None
  }

  /** The array under a chain of filters: a filter is null exactly when its input is. */
  private def root(e: Expression): Expression = e match {
    case ArrayFilter(g, _) => root(g)
    case other => other
  }

  /** A criteria of the form `x is not null`, which `exists()` without criteria builds. */
  private def trivial(f: ArrayFilter): Boolean = f.function match {
    case LambdaFunction(IsNotNull(_: NamedLambdaVariable), _, _) => true
    case _ => false
  }

  /** A filter over an array of structs: the only place where a read of the element is avoidable. */
  private def overStructs(f: ArrayFilter): Boolean = f.argument.dataType match {
    case ArrayType(_: StructType, _) => true
    case _ => false
  }

  /** `CASE WHEN isnull(a) THEN 0 ELSE size(a) END`, which `count()` builds; returns `a`. */
  private def countOf(e: Expression): Option[Expression] = e match {
    case CaseWhen(Seq((IsNull(a), Literal(0, _))), Some(Size(b, _))) if a.semanticEquals(b) =>
      Some(b)
    case _ => None
  }

  /** `transform(g, x -> coalesce(c(x), false))` for the criteria of `f`: one boolean per element. */
  private def matches(f: ArrayFilter): Expression = f.function match {
    case LambdaFunction(body, vars, hidden) =>
      ArrayTransform(f.argument, LambdaFunction(Coalesce(Seq(body, Literal(false))), vars, hidden))
  }

  /** `coalesce(agg(matches(f)), dflt)`: `any` is `array_max` with false, `all` is `array_min` with true. */
  private def aggregate(f: ArrayFilter, agg: Expression => Expression, dflt: Boolean): Expression =
    Coalesce(Seq(agg(matches(f)), Literal(dflt)))

  /** `size(filter(matches(f), b -> b))`: the count of true criteria, reading only the criteria. */
  private def countMatches(f: ArrayFilter, legacy: Boolean): Expression = {
    val v = NamedLambdaVariable("b", BooleanType, nullable = true)
    Size(ArrayFilter(matches(f), LambdaFunction(v, Seq(v))), legacy)
  }

  private def rangeUsed(plan: LogicalPlan): Boolean =
    plan.flatMap(_.expressions).exists(_.find {
      case g: GetStructField => g.name.contains("range")
      case _ => false
    }.isDefined)

  private def isName(e: Expression, n: String): Boolean = e match {
    case Literal(s, StringType) => s.toString == n
    case _ => false
  }

  /** Applies the existence rewrites (`existence`) and the element-shape rewrite (`layer1`). */
  def rewritePlan(plan: LogicalPlan, existence: Boolean, layer1: Boolean): LogicalPlan = {
    val dropRange = layer1 && !rangeUsed(plan)
    plan.transformAllExpressions {
      case EqualTo(l, r) if existence && countOf(l).isDefined && countOf(r).isDefined &&
          (countOf(l).get match {
            case ArrayFilter(g, _) => g.semanticEquals(countOf(r).get)
            case _ => false
          }) =>
        bump("E3-all")
        aggregate(countOf(l).get.asInstanceOf[ArrayFilter], ArrayMin(_), dflt = true)
      case EqualTo(Size(f: ArrayFilter, _), Literal(0, _))
          if existence && !trivial(f) && overStructs(f) =>
        bump("E2-exists")
        Not(aggregate(f, ArrayMax(_), dflt = false))
      case IsNull(f: ArrayFilter) if existence && !trivial(f) && overStructs(f) =>
        bump("E2-isnull")
        IsNull(matches(f))
      case Size(f: ArrayFilter, legacy) if existence && !trivial(f) && overStructs(f) =>
        bump("E2-size")
        countMatches(f, legacy)
      case IsNull(f: ArrayFilter) if existence && leafOf(root(f)).isDefined =>
        bump("E1-isnull")
        IsNull(leafOf(root(f)).get)
      case Size(f: ArrayFilter, legacy) if existence && leafOf(f).isDefined =>
        bump("E1-size")
        Size(leafOf(f).get, legacy)
      case CreateNamedStruct(Seq(n1, v, n2, r))
          if dropRange && isName(n1, "value") && isName(n2, "range") &&
            !(r.isInstanceOf[Literal] && r.asInstanceOf[Literal].value == null) =>
        bump("L1-range")
        CreateNamedStruct(Seq(n1, v, n2, Literal(null, r.dataType)))
    }
  }

  /** Runs the rewritten analysed plan of `api`, with the compute-first rewrite when asked. */
  def run(api: Dataset[Row], existence: Boolean, layer1: Boolean, computeFirst: Boolean)
      : Dataset[Row] = {
    var plan = rewritePlan(qe(api).analyzed, existence, layer1)
    if (computeFirst) plan = PlanRewrite.rewritePlan(plan)
    classic.Dataset.ofRows(api.sparkSession.asInstanceOf[classic.SparkSession], plan)
  }

  /** The leaves a query reads, as a set. */
  def leafSet(df: Dataset[Row]): java.util.Set[String] = {
    val s = new java.util.TreeSet[String]()
    spike.CaseProbe.leaves(spike.CaseProbe.readSchema(df), "").foreach(s.add)
    s
  }

  def rewrittenPlan(api: Dataset[Row], existence: Boolean, layer1: Boolean): String =
    rewritePlan(qe(api).analyzed, existence, layer1).treeString
}
