/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package au.csiro.pathling.sql

import au.csiro.pathling.encoders.{UnevaluableCopy, UnresolvedColumnOrNull}
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, Expression, GetArrayStructFields, GetMapValue, GetStructField, LambdaFunction, Literal, NamedLambdaVariable, NonSQLExpression, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{ArrayType, DataType, NullType, StructType}

/**
 * The decisions that extension traversal makes once its input has resolved, shared by the
 * traversal from an element and the traversal from the resource root.
 *
 * Extension traversal is one expression over the parent, with no second input and no flag for the
 * layout (decision 75). Which layout the data is in is read from the resolved parent:
 *
 *  - where the parent carries an `extension` field, which is the new layout, the result is that
 *    field;
 *  - where it carries `_fid` instead, which is the previous layout, the result is the entry for
 *    that `_fid` in the `_extension` table column;
 *  - where it carries neither, the result is a null of the bottom type for a repeating complex
 *    element (FR-055). This is a structure with no extensions on a pruned new-layout table, and
 *    it must not reach for `_extension`, which that table does not have.
 *
 * The result has the shape a direct reference to the `extension` field would have on the new
 * layout: an array of extensions for a singular parent, and an array of such arrays for a
 * repeating one.
 *
 * The `_extension` column is referenced by name only once the parent has shown a `_fid`, through
 * the tolerant table-column reference, which the analyzer resolves on a later pass. A
 * `RuntimeReplaceable` cannot hold an unresolved reference in its replacement (T009a), so the
 * reference is a child of [[ExtensionLookup]] instead. Where the table has no `_extension` column
 * the lookup finds no extensions (decision 79). That is the case for a structure the engine builds
 * itself, whose `_fid` is always null, over a new-layout table, and for previous-layout data encoded
 * with extensions disabled.
 */
object ExtensionTraversal {

  /** The name of the inline extension field in the new layout. */
  val EXTENSION_FIELD = "extension"

  /** The name of the field identifier in the previous layout. */
  val FID_FIELD = "_fid"

  /** The name of the table column holding the previous layout's extension map. */
  val EXTENSION_MAP_COLUMN = "_extension"

  /** The type of the result where there are no extensions to find. */
  val ABSENT_TYPE: DataType = ArrayType(NullType)

  /**
   * Decides extension traversal over a resolved element.
   *
   * @param parent the resolved structure, or array of structures, whose extensions are wanted
   * @return the expression to replace the traversal with
   */
  def fromElement(parent: Expression): Expression = parent.dataType match {
    case struct: StructType if has(struct, EXTENSION_FIELD) =>
      GetStructField(parent, struct.fieldIndex(EXTENSION_FIELD), Some(EXTENSION_FIELD))
    case ArrayType(struct: StructType, containsNull) if has(struct, EXTENSION_FIELD) =>
      val ordinal = struct.fieldIndex(EXTENSION_FIELD)
      val field = struct.fields(ordinal)
      GetArrayStructFields(parent, field, ordinal, struct.length, containsNull || field.nullable)
    case struct: StructType if has(struct, FID_FIELD) =>
      ExtensionLookup(parent, tolerantColumn(EXTENSION_MAP_COLUMN))
    case ArrayType(struct: StructType, _) if has(struct, FID_FIELD) =>
      ExtensionLookup(parent, tolerantColumn(EXTENSION_MAP_COLUMN))
    case _ =>
      absent
  }

  /**
   * Decides extension traversal at the resource root, from the resolved `_fid` table column.
   *
   * @param fid the resolved `_fid` column, or the fallback null if it is absent
   * @return the expression to replace the traversal with
   */
  def fromRootFid(fid: Expression): Expression =
    if (isAbsent(fid)) {
      absent
    } else {
      ExtensionLookup(fid, tolerantColumn(EXTENSION_MAP_COLUMN))
    }

  /**
   * Returns true where a tolerant table-column reference has fallen back, rather than resolving to
   * the column. The references made here fall back to a null of the null type, which no column
   * resolves to.
   *
   * @param resolved the resolved tolerant reference
   * @return true if the column was absent
   */
  def isAbsent(resolved: Expression): Boolean = resolved match {
    case Literal(null, NullType) => true
    case _ => false
  }

  /** Creates a tolerant reference to a table column, falling back to a null of the null type. */
  def tolerantColumn(name: String): Expression = UnresolvedColumnOrNull(name, NullType)

  /** The result where there are no extensions to find. */
  def absent: Expression = Literal(null, ABSENT_TYPE)

  private def has(struct: StructType, name: String): Boolean = struct.fieldNames.contains(name)
}

/**
 * Traverses to the extensions of an element, on either layout. See [[ExtensionTraversal]] for the
 * decisions it makes.
 *
 * It waits for its parent to resolve, because which layout the data is in is read from the
 * parent's type, and then replaces itself inside `mapChildren`, as
 * `au.csiro.pathling.encoders.UnresolvedVariantUnwrap` does. The parent may be a lambda variable,
 * which resolves later than any attribute.
 *
 * @param parent the structure, or array of structures, whose extensions are wanted
 */
case class UnresolvedTraverseExtension(parent: Expression)
  extends Expression with UnevaluableCopy with NonSQLExpression {

  override def mapChildren(f: Expression => Expression): Expression = {
    val newParent = f(parent)
    if (newParent.resolved) {
      ExtensionTraversal.fromElement(newParent)
    } else {
      copy(parent = newParent)
    }
  }

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override lazy val resolved = false

  override def toString: String = s"traverseExtension($parent)"

  override def children: Seq[Expression] = parent :: Nil

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(parent = newChildren.head)
}

/**
 * Traverses to the extensions of the resource itself, on either layout.
 *
 * At the root there is no parent structure to inspect: every element of the resource is a table
 * column. So the `extension` column is reached through the tolerant table-column reference. Where it
 * resolves, the data is in the new layout and the result is that column. Where it is absent, the
 * traversal continues as [[UnresolvedTraverseRootExtensionByFid]], which reaches the resource's
 * own `_fid` in the same way.
 *
 * The root has its own entry point, rather than being recognised from its parent, because the
 * resource's representation is a boolean existence column, and a boolean primitive element or
 * literal must not be mistaken for it.
 *
 * @param extension the tolerant reference to the `extension` table column
 */
case class UnresolvedTraverseRootExtension(extension: Expression)
  extends Expression with UnevaluableCopy with NonSQLExpression {

  def this() = this(ExtensionTraversal.tolerantColumn(ExtensionTraversal.EXTENSION_FIELD))

  override def mapChildren(f: Expression => Expression): Expression = {
    val newExtension = f(extension)
    if (!newExtension.resolved) {
      copy(extension = newExtension)
    } else if (ExtensionTraversal.isAbsent(newExtension)) {
      UnresolvedTraverseRootExtensionByFid(
        ExtensionTraversal.tolerantColumn(ExtensionTraversal.FID_FIELD))
    } else {
      newExtension
    }
  }

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override lazy val resolved = false

  override def toString: String = "traverseRootExtension()"

  override def children: Seq[Expression] = extension :: Nil

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(extension = newChildren.head)
}

/**
 * The second step of [[UnresolvedTraverseRootExtension]], taken where the resource has no
 * `extension` column. It reaches the resource's `_fid` column through the tolerant table-column
 * reference, and looks it up in `_extension` where it is present.
 *
 * @param fid the tolerant reference to the `_fid` table column
 */
case class UnresolvedTraverseRootExtensionByFid(fid: Expression)
  extends Expression with UnevaluableCopy with NonSQLExpression {

  override def mapChildren(f: Expression => Expression): Expression = {
    val newFid = f(fid)
    if (newFid.resolved) {
      ExtensionTraversal.fromRootFid(newFid)
    } else {
      copy(fid = newFid)
    }
  }

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override lazy val resolved = false

  override def toString: String = "traverseRootExtensionByFid()"

  override def children: Seq[Expression] = fid :: Nil

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(fid = newChildren.head)
}

/**
 * Looks up the previous layout's extensions for an element in the extension map.
 *
 * This is the binary form that T038m showed survives the analyzer on T009a's terms. Both inputs are
 * children, so the analyzer resolves each of them, including an outer reference to the map inside
 * a lambda, and the replacement is built only from resolved nodes:
 *
 *  - over a structure, the map's entry for its `_fid`;
 *  - over an array of structures, the entry for each element's `_fid`;
 *  - over anything else, which is the resource's own `_fid` at the root, the entry for that value;
 *  - where the map is absent from the table, no extensions, whatever the element (decision 79).
 *
 * @param left  the element, or the resource's own `_fid`
 * @param right the extension map, keyed by `_fid`, through the tolerant table-column reference
 */
case class ExtensionLookup(left: Expression, right: Expression)
  extends RuntimeReplaceable with BinaryLike[Expression] {

  override lazy val replacement: Expression = left.dataType match {
    case _ if ExtensionTraversal.isAbsent(right) =>
      ExtensionTraversal.absent
    case struct: StructType =>
      GetMapValue(right, fidOf(left, struct))
    case ArrayType(struct: StructType, containsNull) =>
      val element = NamedLambdaVariable("element", struct, containsNull)
      ArrayTransform(left, LambdaFunction(GetMapValue(right, fidOf(element, struct)), Seq(element)))
    case _ =>
      GetMapValue(right, left)
  }

  override def prettyName: String = "extension_lookup"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)

  private def fidOf(element: Expression, struct: StructType): Expression = {
    val ordinal = struct.fieldIndex(ExtensionTraversal.FID_FIELD)
    GetStructField(element, ordinal, Some(ExtensionTraversal.FID_FIELD))
  }
}
