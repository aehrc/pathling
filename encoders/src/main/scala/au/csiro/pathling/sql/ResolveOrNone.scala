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

import org.apache.spark.sql.catalyst.expressions.{ArraysZip, Coalesce, CreateNamedStruct, Expression, GetArrayStructFields, GetStructField, Literal, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, StructType}

/**
 * A flat structure of some content and those of the named fields of a source that the source's
 * resolved type carries (decisions 142 and 152, design §4.3).
 *
 * The named fields are annotations. A field the source's type lacks is left out of the result
 * entirely, never filled with null, because a null stored annotation is read as an absent value;
 * an accessor then computes it where an operation reads it. Which fields are present is decided
 * from the resolved source, once for the schema, never per row (FR-023). Nothing is computed.
 *
 * In its singular form the source is a structure, and the result is
 * `struct(<content>, <the present fields of source>)`. In its repeating form the source is an array
 * of structures aligned by position with content that is arrays, and the result is
 * `arrays_zip(<content>, <the present fields of every source element>)`, each extracted field
 * guarded with `coalesce(field, array())` so that an absent array of sources pads its annotations
 * with nulls rather than nulling the pairing. Either form names its fields after the content and
 * the annotations, so the two forms have the same fields. A source of any other type, including
 * the bottom type an absent container resolves to, carries none of the fields.
 *
 * @param source       the structure, or array of structures, to take the named fields from
 * @param fieldNames   the names of the fields to take, where present
 * @param content      the content of the result, in order
 * @param contentNames the names of the content fields, in the same order
 * @param repeating    whether the source and the content are arrays to be paired by position
 */
case class ResolveOrNone(source: Expression, fieldNames: Seq[String], content: Seq[Expression],
    contentNames: Seq[String], repeating: Boolean) extends RuntimeReplaceable {

  override lazy val replacement: Expression = {
    val present: Seq[(String, Expression)] = if (repeating) {
      source.dataType match {
        case ArrayType(struct: StructType, containsNull) =>
          fieldNames.filter(struct.fieldNames.contains).map { name =>
            val ordinal = struct.fieldIndex(name)
            val field = struct.fields(ordinal)
            val extracted = GetArrayStructFields(source, field, ordinal, struct.length,
              containsNull || field.nullable)
            name -> Coalesce(Seq(extracted,
              Literal(new GenericArrayData(Array.empty[Any]), extracted.dataType)))
          }
        case _ => Seq.empty
      }
    } else {
      source.dataType match {
        case struct: StructType =>
          fieldNames.filter(struct.fieldNames.contains).map { name =>
            name -> GetStructField(source, struct.fieldIndex(name), Some(name))
          }
        case _ => Seq.empty
      }
    }
    val fields = contentNames.zip(content) ++ present
    if (repeating) {
      ArraysZip(fields.map(_._2), fields.map(field => Literal(field._1)))
    } else {
      CreateNamedStruct(fields.flatMap(field => Seq(Literal(field._1), field._2)))
    }
  }

  override def children: Seq[Expression] = source +: content

  override def prettyName: String = "resolve_or_none"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(source = newChildren.head, content = newChildren.tail)
}
