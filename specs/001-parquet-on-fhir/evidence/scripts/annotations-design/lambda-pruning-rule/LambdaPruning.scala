package spike

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * What is needed of a value. `all` means every field. Otherwise `fields` names the struct fields
 * needed, each with its own requirement. Arrays are transparent: a requirement on an array is a
 * requirement on each of its elements. `Req(false, Map())` is "presence": only whether the value
 * is null (and, for an array, its length), which any one leaf of the value answers.
 */
final case class Req(all: Boolean, fields: Map[String, Req]) {
  def merge(o: Req): Req =
    if (all || o.all) Req.All
    else
      Req(false, (fields.keySet ++ o.fields.keySet).iterator.map { k =>
        k -> ((fields.get(k), o.fields.get(k)) match {
          case (Some(a), Some(b)) => a.merge(b)
          case (Some(a), None) => a
          case (None, Some(b)) => b
          case _ => throw new IllegalStateException
        })
      }.toMap)

  override def toString: String =
    if (all) "*" else fields.map { case (k, v) => s"$k:$v" }.mkString("{", ",", "}")
}

object Req {
  val All: Req = Req(all = true, Map.empty)
  val Presence: Req = Req(all = false, Map.empty)
  def field(name: String, r: Req): Req = Req(all = false, Map(name -> r))
}

/** Backward demand analysis over expressions, keyed by the exprId of attributes and lambda vars. */
final class Demand {
  val acc: mutable.Map[ExprId, Req] = mutable.Map()
  /** Names of expressions that forced a whole read, for diagnostics. */
  val wholeBecause: mutable.Set[String] = mutable.Set()

  /** The demand on each struct constructor instance, so unread fields can be dropped from it. */
  val structs: java.util.IdentityHashMap[CreateNamedStruct, Req] = new java.util.IdentityHashMap()

  def add(id: ExprId, r: Req): Unit = acc(id) = acc.get(id).map(_.merge(r)).getOrElse(r)
  def get(id: ExprId): Option[Req] = acc.get(id)

  private def lambdaVar(f: Expression, i: Int): Option[NamedLambdaVariable] = f match {
    case LambdaFunction(_, args, _) if args.size > i =>
      args(i) match {
        case v: NamedLambdaVariable => Some(v)
        case _ => None
      }
    case _ => None
  }

  private def body(f: Expression): Expression = f.asInstanceOf[LambdaFunction].function

  def visit(e: Expression, r: Req): Unit = e match {
    case a: AttributeReference => add(a.exprId, r)
    case v: NamedLambdaVariable => add(v.exprId, r)
    case Alias(c, _) => visit(c, r)
    case g @ GetStructField(c, ord, _) =>
      visit(c, Req.field(c.dataType.asInstanceOf[StructType](ord).name, r))
    case GetArrayStructFields(c, f, _, _, _) => visit(c, Req.field(f.name, r))
    case s: CreateNamedStruct =>
      structs.put(s, Option(structs.get(s)).map(_.merge(r)).getOrElse(r))
      s.nameExprs.zip(s.valExprs).foreach { case (n, v) =>
        val name = n.eval().toString
        if (r.all) visit(v, Req.All) else r.fields.get(name).foreach(visit(v, _))
      }
    case CreateArray(cs, _) => cs.foreach(visit(_, r))
    case c: Concat if c.dataType.isInstanceOf[ArrayType] => c.children.foreach(visit(_, r))
    case Flatten(c) => visit(c, r)
    case ea: ElementAt if ea.left.dataType.isInstanceOf[ArrayType] =>
      visit(ea.left, r); visit(ea.right, Req.All)
    case gi: GetArrayItem => visit(gi.child, r); visit(gi.ordinal, Req.All)
    case Size(c, _) => visit(c, Req.Presence)
    case IsNull(c) => visit(c, Req.Presence)
    case IsNotNull(c) => visit(c, Req.Presence)
    case KnownNotNull(c) => visit(c, r)
    case If(p, t, f) => visit(p, Req.All); visit(t, r); visit(f, r)
    case CaseWhen(branches, elseValue) =>
      branches.foreach { case (c, v) => visit(c, Req.All); visit(v, r) }
      elseValue.foreach(visit(_, r))
    case Coalesce(cs) => cs.foreach(visit(_, r))
    case f: ArrayFilter =>
      // The result holds elements of the argument, so the argument owes the result's demand plus
      // whatever the predicate reads of its variable.
      visit(body(f.function), Req.All)
      val v = lambdaVar(f.function, 0).flatMap(x => get(x.exprId)).getOrElse(Req.Presence)
      visit(f.argument, r.merge(v))
    case t: ArrayTransform =>
      visit(body(t.function), r)
      visit(t.argument, lambdaVar(t.function, 0).flatMap(x => get(x.exprId)).getOrElse(Req.Presence))
    case x: ArrayExists =>
      visit(body(x.function), Req.All)
      visit(x.argument, lambdaVar(x.function, 0).flatMap(v => get(v.exprId)).getOrElse(Req.Presence))
    case x: ArrayForAll =>
      visit(body(x.function), Req.All)
      visit(x.argument, lambdaVar(x.function, 0).flatMap(v => get(v.exprId)).getOrElse(Req.Presence))
    case z: ZipWith =>
      // Pairs by position; the result is as long as the longer input, so both owe presence.
      visit(body(z.function), r)
      visit(z.left, lambdaVar(z.function, 0).flatMap(v => get(v.exprId)).getOrElse(Req.Presence)
        .merge(Req.Presence))
      visit(z.right, lambdaVar(z.function, 1).flatMap(v => get(v.exprId)).getOrElse(Req.Presence)
        .merge(Req.Presence))
    case z: ArraysZip =>
      z.children.zip(z.names).foreach { case (c, n) =>
        val name = n.eval().toString
        val own = if (r.all) Req.All else r.fields.getOrElse(name, Req.Presence)
        visit(c, own.merge(Req.Presence))
      }
    case a: ArrayAggregate if lambdaVar(a.merge, 1).isDefined =>
      // The accumulator owes what finish reads of it plus what merge reads of it; merge's result
      // becomes the accumulator, so iterate to a fixed point.
      visit(body(a.finish), r)
      val accM = lambdaVar(a.merge, 0).get.exprId
      var acc = lambdaVar(a.finish, 0).flatMap(v => get(v.exprId)).getOrElse(Req.Presence)
      var stable = false
      var i = 0
      while (!stable && i < 8) {
        visit(body(a.merge), acc)
        val next = acc.merge(get(accM).getOrElse(Req.Presence))
        stable = next == acc
        acc = next
        i += 1
      }
      if (!stable) acc = Req.All
      visit(body(a.merge), acc)
      visit(a.zero, acc)
      visit(a.argument, lambdaVar(a.merge, 1).flatMap(v => get(v.exprId)).getOrElse(Req.Presence))
    case other =>
      if (other.children.exists(c => isNested(c.dataType))) wholeBecause += other.prettyName
      other.children.foreach(visit(_, Req.All))
  }

  private def isNested(t: DataType): Boolean = t match {
    case _: StructType => true
    case ArrayType(et, _) => isNested(et)
    case _ => false
  }
}

/**
 * Prunes the nested fields of Parquet scans to what the plan reads, following reads through
 * higher-order functions, `flatten`, struct and array construction, and `Generate`. Spark's own
 * `SchemaPruning` follows only field extractions applied directly to a column.
 *
 * The rule does the scan rewrite itself, so it does not depend on running before `SchemaPruning`.
 */
object LambdaPruning extends Rule[LogicalPlan] with Logging {

  @volatile var verbose: Boolean = false

  /** Adds the rule to a running session; takes effect for every query planned afterwards. */
  def install(spark: SparkSession): Unit =
    if (!spark.experimental.extraOptimizations.contains(this))
      spark.experimental.extraOptimizations = spark.experimental.extraOptimizations :+ this

  def uninstall(spark: SparkSession): Unit =
    spark.experimental.extraOptimizations = spark.experimental.extraOptimizations.filterNot(_ eq this)

  // ---- Demand over the plan. ----

  private def analyse(plan: LogicalPlan): Demand = {
    val d = new Demand
    plan.output.foreach(a => d.add(a.exprId, Req.All))
    def go(p: LogicalPlan): Unit = {
      p match {
        case Project(list, _) =>
          list.foreach {
            case a: Alias => d.get(a.exprId).foreach(r => d.visit(a.child, r))
            case _: Attribute => () // Pass-through: its demand arrives under the same exprId.
            case other => d.visit(other, Req.All)
          }
        case Filter(cond, _) => d.visit(cond, Req.All)
        case g: Generate =>
          val names = g.generator.elementSchema.fieldNames
          val byField = g.generatorOutput.zip(names).flatMap { case (a, n) =>
            d.get(a.exprId).map(n -> _)
          }.toMap
          g.generator match {
            case Inline(c) => d.visit(c, Req(all = false, byField))
            case Explode(c) if c.dataType.isInstanceOf[ArrayType] =>
              d.visit(c, byField.getOrElse("col", Req.Presence))
            case PosExplode(c) if c.dataType.isInstanceOf[ArrayType] =>
              d.visit(c, byField.getOrElse("col", Req.Presence))
            case other => other.children.foreach(d.visit(_, Req.All))
          }
        case _: LeafNode => ()
        case _ if p.output.forall(a => p.children.exists(_.outputSet.contains(a))) =>
          // An operator that passes attributes through (Sort, Join, Limit, ...): only its own
          // expressions add demand.
          p.expressions.foreach(d.visit(_, Req.All))
        case _ =>
          // Anything else (Aggregate, Union, Window, Expand, ...): everything it reads is whole.
          p.expressions.foreach(d.visit(_, Req.All))
          p.children.foreach(_.output.foreach(a => d.add(a.exprId, Req.All)))
      }
      p.children.foreach(go)
    }
    go(plan)
    d
  }

  // ---- Pruning a type to a requirement. ----

  private def minimal(t: DataType): DataType = t match {
    case st: StructType =>
      val f = st.fields.find(f => !isComplex(f.dataType)).getOrElse(st.fields.head)
      StructType(Seq(f.copy(dataType = minimal(f.dataType))))
    case ArrayType(et, cn) => ArrayType(minimal(et), cn)
    case other => other
  }

  private def isComplex(t: DataType): Boolean =
    t.isInstanceOf[StructType] || t.isInstanceOf[ArrayType] || t.isInstanceOf[MapType]

  def prune(t: DataType, r: Req): DataType =
    if (r.all) t
    else
      t match {
        case st: StructType =>
          val kept = st.fields.filter(f => r.fields.contains(f.name)).map { f =>
            f.copy(dataType = prune(f.dataType, r.fields(f.name)))
          }
          // Never an empty struct: a value needed only for its nullness keeps one leaf.
          if (kept.isEmpty) minimal(st) else StructType(kept)
        case ArrayType(et, cn) => ArrayType(prune(et, r), cn)
        case other => other
      }

  // ---- Re-typing expressions over the pruned scan. ----

  private final class Retype(types: mutable.Map[ExprId, DataType], d: Demand) {
    def apply(e: Expression, env: Map[ExprId, (DataType, Boolean)]): Expression = e match {
      case s: CreateNamedStruct if d.structs.containsKey(s) && !d.structs.get(s).all =>
        // A field nothing reads becomes a typed null, so that its input is no longer referenced.
        val r = d.structs.get(s)
        CreateNamedStruct(s.nameExprs.zip(s.valExprs).flatMap { case (n, v) =>
          val keep = r.fields.contains(n.eval().toString)
          Seq(n, if (keep) apply(v, env) else Literal(null, droppedType(v, env)))
        })
      case a: AttributeReference =>
        types.get(a.exprId).filter(_ != a.dataType).map(a.withDataType).getOrElse(a)
      case v: NamedLambdaVariable =>
        env.get(v.exprId).map { case (t, n) => v.copy(dataType = t, nullable = n) }.getOrElse(v)
      case GetStructField(c, ord, name) =>
        val fieldName = name.getOrElse(c.dataType.asInstanceOf[StructType](ord).name)
        val nc = apply(c, env)
        val st = nc.dataType.asInstanceOf[StructType]
        GetStructField(nc, st.fieldIndex(fieldName), Some(fieldName))
      case GetArrayStructFields(c, f, _, _, _) =>
        val nc = apply(c, env)
        val ArrayType(st: StructType, cn) = nc.dataType: @unchecked
        val i = st.fieldIndex(f.name)
        GetArrayStructFields(nc, st(i), i, st.length, cn || st(i).nullable)
      case h: HigherOrderFunction =>
        val fns = h.functions.toSet
        val withArgs = h.mapChildren(c => if (fns.contains(c)) c else apply(c, env))
          .asInstanceOf[HigherOrderFunction]
        withArgs.bind { (f, argTypes) =>
          val LambdaFunction(body, args, hidden) = f: @unchecked
          val newArgs = args.zip(argTypes).map {
            case (v: NamedLambdaVariable, (t, n)) => v.copy(dataType = t, nullable = n)
            case (other, _) => other
          }
          val env2 = env ++ newArgs.collect { case v: NamedLambdaVariable =>
            v.exprId -> (v.dataType, v.nullable)
          }
          LambdaFunction(apply(body, env2), newArgs, hidden)
        }
      case other => other.mapChildren(apply(_, env))
    }

    /**
     * The type a dropped struct field keeps. Its value is never read, but its type should match
     * what the same field has in sibling constructors (for concat, array, CASE), so it is
     * re-typed where that is possible. It cannot be where it reads fields that were pruned away
     * because nothing reads them; then the original type stands, and any mismatch that causes
     * is caught by the type check at the end.
     */
    def droppedType(v: Expression, env: Map[ExprId, (DataType, Boolean)]): DataType =
      try apply(v, env).dataType
      catch { case _: Exception => v.dataType }
  }

  // ---- The rule. ----

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConfString("spike.lambdaPruning.enabled", "true").toBoolean) return plan
    try rewrite(plan)
    catch {
      case e: Exception =>
        logWarning(s"LambdaPruning bailed out: $e")
        if (verbose) {
          System.err.println(s"LambdaPruning bailed out: $e")
          e.getStackTrace.take(12).foreach(f => System.err.println(s"    at $f"))
          System.err.println(s"  on plan:\n$plan")
        }
        plan
    }
  }

  private def isParquet(l: LogicalRelation): Boolean = l.relation match {
    case fs: HadoopFsRelation => fs.fileFormat.isInstanceOf[ParquetFileFormat]
    case _ => false
  }

  private def rewrite(plan: LogicalPlan): LogicalPlan = {
    val relations = plan.collect { case l: LogicalRelation if isParquet(l) => l }
    if (relations.isEmpty) return plan
    val d = analyse(plan)

    // Pruned types for the scans' attributes. An attribute nothing reads keeps its type.
    val types = mutable.Map[ExprId, DataType]()
    var narrowed = false
    for (l <- relations; a <- l.output; r <- d.get(a.exprId)) {
      val t = prune(a.dataType, r)
      if (t != a.dataType) { types(a.exprId) = t; narrowed = true }
    }
    // Pass-through entries of a Project that nothing above reads are dropped below too, since
    // they would otherwise pin a whole column (the Project that ColumnPruning leaves under a
    // Generate).
    val unread = plan.collect { case Project(list, _) =>
      list.collect { case a: Attribute if d.get(a.exprId).isEmpty => a.exprId }
    }.flatten.toSet
    if (!narrowed && unread.isEmpty) return plan
    if (verbose) {
      System.err.println(s"LambdaPruning: demand ${relations.flatMap(_.output).flatMap(a =>
        d.get(a.exprId).map(r => s"${a.name}=$r")).mkString(", ")}")
      if (d.wholeBecause.nonEmpty)
        System.err.println(s"LambdaPruning: whole because of ${d.wholeBecause.mkString(", ")}")
    }

    val retype = new Retype(types, d)
    def record(p: LogicalPlan): LogicalPlan = {
      p.output.foreach(a => types(a.exprId) = a.dataType); p
    }
    def rw(p: LogicalPlan): LogicalPlan = p match {
      case l: LogicalRelation if isParquet(l) =>
        val fs = l.relation.asInstanceOf[HadoopFsRelation]
        val newOutput = l.output.map(a => types.get(a.exprId).map(a.withDataType).getOrElse(a))
        val byName = newOutput.map(a => a.name -> a.dataType).toMap
        val newData = StructType(fs.dataSchema.fields.map(f =>
          byName.get(f.name).map(t => f.copy(dataType = t)).getOrElse(f)))
        if (newData == fs.dataSchema) l
        else {
          val newFs = fs.copy(dataSchema = newData)(fs.sparkSession)
          val n = l.copy(relation = newFs, output = newOutput)
          n.copyTagsFrom(l)
          n
        }
      case g: Generate =>
        val required = g.requiredChildOutput.map(_.exprId).toSet
        val nc = rw(g.child)
        val unrequired = nc.output.zipWithIndex.collect {
          case (a, i) if !required.contains(a.exprId) => i
        }
        val gen = retype(g.generator, Map.empty).asInstanceOf[Generator]
        val out = g.generatorOutput.zip(gen.elementSchema.fields).map {
          case (a: AttributeReference, f) => a.withDataType(f.dataType)
          case (a, _) => a
        }
        record(g.copy(generator = gen, unrequiredChildIndex = unrequired, generatorOutput = out,
          child = nc))
      case Project(list, child) =>
        val nc = rw(child)
        val kept = list.filter {
          case a: Attribute => !unread.contains(a.exprId)
          case _ => true
        }
        record(Project(kept.map(retype(_, Map.empty).asInstanceOf[NamedExpression]), nc))
      case other =>
        val n = other.withNewChildren(other.children.map(rw)).mapExpressions(retype(_, Map.empty))
        record(n)
    }
    val result = rw(plan)

    // Guards: the result must be resolved, type-check, and have the original output types.
    if (!result.resolved) throw new IllegalStateException("result not resolved")
    if (result.schema != plan.schema)
      throw new IllegalStateException(s"output schema changed: ${result.schema} vs ${plan.schema}")
    result.foreach(_.expressions.foreach(_.foreach { e =>
      val c = e.checkInputDataTypes()
      if (c.isFailure) throw new IllegalStateException(s"type check failed for ${e.prettyName}: $c")
    }))
    if (verbose) System.err.println(s"LambdaPruning: rewrote\n$result")
    result
  }
}
