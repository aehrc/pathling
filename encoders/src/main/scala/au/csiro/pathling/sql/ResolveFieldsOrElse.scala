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

import org.apache.spark.sql.catalyst.expressions.{ArraysZip, CreateNamedStruct, Expression, GetArrayStructFields, GetStructField, Literal, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{ArrayType, StructType}

/**
 * A structure of several fields of its child that resolves to those fields where the child's
 * resolved type carries all of them, and to a fallback expression where it does not.
 *
 * This is [[ResolveOrElse]] for an annotation kind carried in more than one field, such as the two
 * bounds of a date's range: the fallback computes all of them at once, so a kind whose fields are
 * absent is computed once rather than once for each field. Presence is decided from the resolved
 * input, once for the schema, never per row (FR-023).
 *
 * Where the child carries the fields, the result is a structure of them, named as they are, which
 * is not null even where the child is. The child may also be an array of structures, in which case
 * the result is an array of such structures, one for each element, and the fallback is expected to
 * be an array aligned with it. Any other child type, including the bottom type an absent container
 * resolves to, yields the fallback, which is expected to be a structure of the same fields, or an
 * array of them.
 *
 * @param child      the structure, or array of structures, to take the fields from
 * @param fieldNames the names of the fields
 * @param fallback   the structure taken when any of the fields is absent
 */
case class ResolveFieldsOrElse(child: Expression, fieldNames: Seq[String], fallback: Expression)
  extends RuntimeReplaceable with BinaryLike[Expression] {

  override def left: Expression = child

  override def right: Expression = fallback

  override lazy val replacement: Expression = child.dataType match {
    case struct: StructType if fieldNames.forall(struct.fieldNames.contains) =>
      CreateNamedStruct(fieldNames.flatMap { name =>
        Seq(Literal(name), GetStructField(child, struct.fieldIndex(name), Some(name)))
      })
    case ArrayType(struct: StructType, containsNull)
      if fieldNames.forall(struct.fieldNames.contains) =>
      ArraysZip(fieldNames.map { name =>
        val ordinal = struct.fieldIndex(name)
        val field = struct.fields(ordinal)
        GetArrayStructFields(child, field, ordinal, struct.length, containsNull || field.nullable)
      }, fieldNames.map(Literal(_)))
    case _ =>
      fallback
  }

  override def prettyName: String = "resolve_fields_or_else"

  override protected def withNewChildrenInternal(newLeft: Expression,
                                                 newRight: Expression): Expression =
    copy(child = newLeft, fallback = newRight)
}
