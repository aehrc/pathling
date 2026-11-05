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

import au.csiro.pathling.encoders.ExtensionSupport.EXTENSION_ELEMENT_NAME
import au.csiro.pathling.encoders.UnevaluableCopy
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.{Expression, GetArrayStructFields, GetStructField, Literal, NonSQLExpression}
import org.apache.spark.sql.types.{ArrayType, DataType, NullType, StructType}

/**
 * The decisions that extension traversal makes once its input has resolved.
 *
 * Extension traversal is one expression over the parent, with no second input (decision 75). The
 * extensions are read from the resolved parent:
 *
 *  - where the parent carries an `extension` field, the result is that field;
 *  - where it carries none, the result is a null of the bottom type for a repeating complex element
 *    (FR-055). This is a structure with no extensions on a pruned table.
 *
 * A System value the engine builds itself has no `extension` field, so it takes the second branch:
 * a System type has no extensions.
 *
 * The result has the shape a direct reference to the `extension` field would have: an array of
 * extensions for a singular parent, and an array of such arrays for a repeating one.
 */
object ExtensionTraversal {

  /** The type of the result where there are no extensions to find. */
  val ABSENT_TYPE: DataType = ArrayType(NullType)

  /**
   * Decides extension traversal over a resolved element.
   *
   * @param parent the resolved structure, or array of structures, whose extensions are wanted
   * @return the expression to replace the traversal with
   */
  def fromElement(parent: Expression): Expression = parent.dataType match {
    case struct: StructType if has(struct, EXTENSION_ELEMENT_NAME) =>
      GetStructField(
        parent, struct.fieldIndex(EXTENSION_ELEMENT_NAME), Some(EXTENSION_ELEMENT_NAME))
    case ArrayType(struct: StructType, containsNull) if has(struct, EXTENSION_ELEMENT_NAME) =>
      val ordinal = struct.fieldIndex(EXTENSION_ELEMENT_NAME)
      val field = struct.fields(ordinal)
      GetArrayStructFields(parent, field, ordinal, struct.length, containsNull || field.nullable)
    case _ =>
      absent
  }

  /** The result where there are no extensions to find. */
  def absent: Expression = Literal(null, ABSENT_TYPE)

  private def has(struct: StructType, name: String): Boolean = struct.fieldNames.contains(name)
}

/**
 * Traverses to the extensions of an element. See [[ExtensionTraversal]] for the decisions it makes.
 *
 * It waits for its parent to resolve, because the extensions are found from the parent's type, and
 * then replaces itself inside `mapChildren`, as `au.csiro.pathling.encoders.UnresolvedVariantUnwrap`
 * does. The parent may be a lambda variable, which resolves later than any attribute.
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
