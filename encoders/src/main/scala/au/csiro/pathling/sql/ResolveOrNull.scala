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

import au.csiro.pathling.sql.DecimalNormalisation.{fieldToText, holdsDecimals}
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, Expression, GetArrayStructFields, GetStructField, LambdaFunction, Literal, NamedLambdaVariable, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

/**
 * The tolerant traversal expression: a reference to a field of its child that resolves to the field
 * where the child's resolved type carries it, and to a null of the declared fallback type where it
 * does not (FR-054).
 *
 * Presence is decided from the resolved input rather than when the expression is built, so one
 * column is valid over every schema that conforms to the definitions. The fallback type is supplied
 * by the caller, from the definitions, following FR-055: a singular primitive takes the
 * definition's type, a repeating primitive an array of it, a singular complex element the bottom
 * type, and a repeating complex element an array of the bottom type.
 *
 * The child may be a structure, or an array of structures, in which case the field is extracted
 * from every element, as a direct field reference over an array does. Any other child type,
 * including the bottom type and an array of it that an absent parent resolves to, yields the
 * fallback. Field names are matched exactly, because FHIR names are case-sensitive.
 *
 * The construction follows the one the T009a spike proved (`evidence/t009a-analyzer-gate.md`):
 *
 *  - `UnaryLike` rather than `InheritAnalysisRules`, so that the traversal target is the child
 *    and resolving it is what the analyzer does;
 *  - `replacement` is a `lazy val` on a case class, so that resolution produces a fresh copy;
 *  - the replacement is built only from resolved nodes, which `CheckAnalysis` requires.
 *
 * The optimiser replaces the expression with its replacement before any pruning rule runs, so a
 * plan using it prunes exactly as one written with a direct field reference.
 *
 * The expression is also the one site at which the previous layout is normalised to the new one
 * (T094b), so that the engine above it sees one layout. Each branch is chosen from the resolved
 * type of the child, once per schema, and every branch yields the type that the same traversal
 * yields over the new layout:
 *
 *  - a decimal field, which the previous layout stores as a `DECIMAL(32,6)` value beside a
 *    `_scale` companion, yields its text at the source scale, as [[DecimalNormalisation]]
 *    describes. The companion is read beside the value, and nothing else is.
 *
 * @param child     the structure, or array of structures, to take the field from
 * @param fieldName the name of the field
 * @param fallback  the type of the null returned when the field is absent
 */
case class ResolveOrNull(child: Expression, fieldName: String, fallback: DataType)
  extends RuntimeReplaceable with UnaryLike[Expression] {

  override lazy val replacement: Expression = child.dataType match {
    case struct: StructType if isDecimal(struct) =>
      fieldToText(child, struct, fieldName)
    case ArrayType(struct: StructType, containsNull) if isDecimal(struct) =>
      val element = NamedLambdaVariable("element", struct, containsNull)
      ArrayTransform(child, LambdaFunction(fieldToText(element, struct, fieldName), Seq(element)))
    case struct: StructType if struct.fieldNames.contains(fieldName) =>
      GetStructField(child, struct.fieldIndex(fieldName), Some(fieldName))
    case ArrayType(struct: StructType, containsNull) if struct.fieldNames.contains(fieldName) =>
      val ordinal = struct.fieldIndex(fieldName)
      val field = struct.fields(ordinal)
      GetArrayStructFields(child, field, ordinal, struct.length, containsNull || field.nullable)
    case _ =>
      Literal(null, fallback)
  }

  private def isDecimal(struct: StructType): Boolean =
    struct.fieldNames.contains(fieldName) && holdsDecimals(struct(fieldName).dataType)

  override def prettyName: String = "resolve_or_null"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
