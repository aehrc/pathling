package org.apache.spark.sql.qroot

import au.csiro.pathling.encoders.{UnevaluableCopy, UnresolvedColumnOrNull}
import org.apache.spark.sql.{AnalysisException, Column}
import org.apache.spark.sql.catalyst.analysis.{GetViewColumnByNameAndOrdinal, UnresolvedAttribute, UnresolvedException}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, UnaryLike}
import org.apache.spark.sql.classic.ExpressionUtils
import org.apache.spark.sql.types._

/** Spark's `private[sql]` bridge between Column and Expression, reachable from this package. */
object Bridge {
  def col(e: Expression): Column = ExpressionUtils.column(e)
  def expr(c: Column): Expression = ExpressionUtils.expression(c)
}

/**
 * Prototype of `columnOrElse(name, fallback)`, written directly on the model of
 * [[au.csiro.pathling.encoders.UnresolvedColumnOrNull]]: the top-level column where the input has it,
 * and the fallback expression where it does not. The absence signal and the guards are the same
 * ones, taken from the original's companion; only the value of the absent branch differs, and the
 * ambiguity branch is left to Spark's own error (the original's `resolveMatches` is private).
 */
case class UnresolvedColumnOrElse(columnName: String, orElse: Expression, value: Expression)
  extends Expression with UnevaluableCopy with NonSQLExpression {

  def this(columnName: String, orElse: Expression) =
    this(columnName, orElse,
      GetViewColumnByNameAndOrdinal(UnresolvedColumnOrNull.SOURCE_NAME, columnName, 0, 1, None))

  override def mapChildren(f: Expression => Expression): Expression =
    f(UnresolvedAttribute(Seq(columnName))) match {
      case attribute: Attribute if attribute.resolved => attribute
      case _ =>
        try {
          val newValue = f(value)
          if (newValue.resolved) newValue else copy(orElse = f(orElse), value = newValue)
        } catch {
          case e: AnalysisException if e.getCondition == "INCOMPATIBLE_VIEW_SCHEMA_CHANGE" &&
            UnresolvedColumnOrNull.NO_MATCH == e.getMessageParameters.get("actualCols") =>
            f(orElse)
          case e: AssertionError if UnresolvedColumnOrNull.isSingleChildAssertion(e) =>
            f(orElse)
        }
    }

  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved = false
  override def toString: String = s"columnOrElse($columnName)"
  // The fallback is a child, so that the analyser converts and resolves it with the rest of the plan.
  override def children: Seq[Expression] = value :: orElse :: Nil
  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(value = newChildren(0), orElse = newChildren(1))
}

/**
 * The composed form of `columnOrElse`: the existing tolerant reference, `columnOrNull(name,
 * NullType)`, as the child, which resolves to the attribute or to a null of the bottom type, and a
 * runtime-replaceable choice between the child and the fallback by that type, as `ResolveOrElse`
 * chooses by the type of its child. It needs no new analyser internals.
 */
case class ColumnOrElse(child: Expression, fallback: Expression)
  extends RuntimeReplaceable with BinaryLike[Expression] {
  override def left: Expression = child
  override def right: Expression = fallback
  override lazy val replacement: Expression =
    if (child.dataType == NullType) fallback else child
  override def prettyName: String = "column_or_else"
  override protected def withNewChildrenInternal(l: Expression, r: Expression): Expression =
    copy(child = l, fallback = r)
}

/**
 * The set form: a structure of whichever of the named top-level columns exist, as its fields. A
 * child that resolved to the bottom type is an absent column and is left out; with every column
 * absent the structure has no fields.
 */
case class ColumnsStruct(fieldNames: Seq[String], override val children: Seq[Expression])
  extends RuntimeReplaceable {
  override lazy val replacement: Expression = CreateNamedStruct(
    fieldNames.zip(children).filter(_._2.dataType != NullType)
      .flatMap { case (n, c) => Seq(Literal(n), c) })
  override def prettyName: String = "columns_struct"
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}

/**
 * The model of `resolve_or_none`: of the listed fields, the ones that the child's resolved struct
 * type carries, as a structure; a structure with no fields where the child has none of them or is
 * the bottom type.
 */
case class ResolveOrNone(child: Expression, fieldNames: Seq[String])
  extends RuntimeReplaceable with UnaryLike[Expression] {
  override lazy val replacement: Expression = child.dataType match {
    case st: StructType =>
      CreateNamedStruct(fieldNames.filter(st.fieldNames.contains).flatMap { n =>
        Seq(Literal(n), GetStructField(child, st.fieldIndex(n), Some(n)))
      })
    case _ => CreateNamedStruct(Nil)
  }
  override def prettyName: String = "resolve_or_none"
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

/** Column-level entry points of the prototypes. */
object Proto {
  import Bridge._
  /** The direct prototype, modelled on `UnresolvedColumnOrNull`. */
  def columnOrElseDirect(name: String, orElse: Column): Column =
    col(new UnresolvedColumnOrElse(name, expr(orElse)))
  /** The composed prototype over the existing `columnOrNull(name, NullType)`. */
  def columnOrElse(name: String, orElse: Column): Column =
    col(ColumnOrElse(expr(au.csiro.pathling.encoders.ColumnFunctions
      .columnOrNull(name, NullType)), expr(orElse)))
  /** A structure of the named top-level columns that exist, keyed by field name. */
  def columnsStruct(entries: Seq[(String, String)]): Column =
    col(ColumnsStruct(entries.map(_._1), entries.map { case (_, column) =>
      expr(au.csiro.pathling.encoders.ColumnFunctions.columnOrNull(column, NullType)) }))
  def resolveOrNone(child: Column, fields: Seq[String]): Column =
    col(ResolveOrNone(expr(child), fields))
  def resolveOrElse(child: Column, field: String, fallback: Column): Column =
    au.csiro.pathling.encoders.ColumnFunctions.resolveOrElse(child, field, fallback)
}
