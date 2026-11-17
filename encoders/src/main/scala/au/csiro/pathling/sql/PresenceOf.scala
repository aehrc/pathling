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

import org.apache.spark.sql.catalyst.expressions.{ArrayFilter, ArrayTransform, Coalesce,
  Expression, GetArrayStructFields, IsNotNull, LambdaFunction, Literal, NamedLambdaVariable,
  RuntimeReplaceable}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{ArrayType, BooleanType, MapType, StructType}

/**
 * An array that stands for another in a test of presence or a count: it is null exactly where the
 * other is, and has as many elements, but is built in the forms that Spark's schema pruning can
 * narrow to the fields they read (decision 143, L2; `evidence/annotations-design-rewrites-layer1.md`,
 * rewrites E1 and E2). Spark 4.0 prunes neither through a lambda; Spark 4.3 prunes both.
 *
 * A test of presence or a count needs the number of elements, not their content, and Spark's
 * schema pruning reads the whole element of an array whose size is taken. So:
 *
 *  - An array filtered by a criteria, `filter(g, x -> c(x))`, as `where()` builds it, stands as
 *    `filter(transform(g, x -> coalesce(c(x), false)), b -> b)`, which reads only what the criteria
 *    reads. A criteria that is null for an element does not keep it, as it does not in the filter.
 *  - Any other array of structures, such as the engine's removal of null elements,
 *    `filter(a, x -> isnotnull(x))`, stands as the array of the first field of its elements that is
 *    neither a structure, an array nor a map, which has an element wherever the array has one. A
 *    null-removal filter is not treated as a criteria, because its criteria reads the whole
 *    element.
 *  - Any other array stands as itself.
 *
 * @param child the array
 */
case class PresenceOf(child: Expression) extends RuntimeReplaceable with UnaryLike[Expression] {

  override lazy val replacement: Expression = child match {
    case filter@ArrayFilter(_, LambdaFunction(IsNotNull(tested: NamedLambdaVariable), Seq(
    element: NamedLambdaVariable), _)) if tested.exprId == element.exprId =>
      leafOf(filter)
    case ArrayFilter(input, LambdaFunction(criteria, arguments, hidden)) =>
      val matched = ArrayTransform(input,
        LambdaFunction(Coalesce(Seq(criteria, Literal(false))), arguments, hidden))
      val kept = NamedLambdaVariable("b", BooleanType, nullable = false)
      ArrayFilter(matched, LambdaFunction(kept, Seq(kept)))
    case _ => leafOf(child)
  }

  /**
   * The array of the first atomic field of an array of structures, or the array itself where its
   * elements are not structures or have no atomic field.
   */
  private def leafOf(array: Expression): Expression = array.dataType match {
    case ArrayType(struct: StructType, containsNull) =>
      struct.fields.indexWhere(field => field.dataType match {
        case _: StructType | _: ArrayType | _: MapType => false
        case _ => true
      }) match {
        case -1 => array
        case ordinal =>
          GetArrayStructFields(array, struct.fields(ordinal), ordinal, struct.length,
            containsNull || struct.fields(ordinal).nullable)
      }
    case _ => array
  }

  override def prettyName: String = "presence_of"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
