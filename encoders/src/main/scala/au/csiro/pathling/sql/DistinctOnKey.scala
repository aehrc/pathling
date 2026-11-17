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

import org.apache.spark.sql.catalyst.expressions.{ArrayAggregate, ArrayContains, ArrayDistinct,
  ArrayExists, ArrayTransform, Coalesce, Concat, CreateArray, CreateNamedStruct, Expression,
  GetStructField, If, IsNull, LambdaFunction, Literal, NamedLambdaVariable, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.ArrayType

/**
 * The elements of an array without duplicates, compared by a key that leaves out the annotation
 * fields the engine does not recognise, keeping the first element of each key (decision 151).
 *
 * The key of each element is computed once: each element is paired with it, and one pass over the
 * pairs keeps an element only where its key has not been seen. A single null element is kept, as
 * Spark's `array_distinct` keeps one. Where the element type carries no
 * annotation field to leave out, as in data Pathling wrote, the key is the element itself, and the
 * expression is Spark's own `array_distinct`. The array is held once either way, so that a chain
 * of unions grows the plan linearly.
 *
 * @param child      the array to deduplicate
 * @param recognised the names of the annotation fields the engine recognises
 */
case class DistinctOnKey(child: Expression, recognised: Seq[String])
  extends RuntimeReplaceable with UnaryLike[Expression] {

  override lazy val replacement: Expression = child.dataType match {
    case ArrayType(element, containsNull)
      if StripAnnotations.stripped(element, recognised) != element =>
      val key = StripAnnotations.stripped(element, recognised)
      val each = NamedLambdaVariable("element", element, containsNull)
      val pairs = ArrayTransform(child, LambdaFunction(CreateNamedStruct(Seq(
        Literal("key"), MergeCast.project(each, element, key),
        Literal("element"), each)), Seq(each)))
      val pairType = pairs.dataType.asInstanceOf[ArrayType].elementType
      val keysType = ArrayType(key, containsNull = true)
      val elementsType = ArrayType(element, containsNull = true)
      val zero = CreateNamedStruct(Seq(
        Literal("keys"), Literal(new GenericArrayData(Array.empty[Any]), keysType),
        Literal("elements"), Literal(new GenericArrayData(Array.empty[Any]), elementsType)))
      val accumulator = NamedLambdaVariable("seen", zero.dataType, nullable = false)
      val pair = NamedLambdaVariable("pair", pairType, nullable = false)
      val seenKeys = GetStructField(accumulator, 0)
      val pairKey = GetStructField(pair, 0)
      // A null element has a null key, which array_contains cannot find, so a null key is seen
      // where the keys seen hold a null: a single null element is kept, as array_distinct keeps.
      val seenKey = NamedLambdaVariable("seenKey", key, nullable = true)
      val seen = If(IsNull(pairKey),
        ArrayExists(seenKeys, LambdaFunction(IsNull(seenKey), Seq(seenKey))),
        Coalesce(Seq(ArrayContains(seenKeys, pairKey), Literal(false))))
      val merge = If(seen,
        accumulator,
        CreateNamedStruct(Seq(
          Literal("keys"), Concat(Seq(seenKeys, CreateArray(Seq(pairKey)))),
          Literal("elements"), Concat(Seq(GetStructField(accumulator, 1),
            CreateArray(Seq(GetStructField(pair, 1))))))))
      val finished = NamedLambdaVariable("seen", zero.dataType, nullable = false)
      ArrayAggregate(pairs, zero, LambdaFunction(merge, Seq(accumulator, pair)),
        LambdaFunction(GetStructField(finished, 1), Seq(finished)))
    case _ => ArrayDistinct(child)
  }

  override def prettyName: String = "distinct_on_key"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
