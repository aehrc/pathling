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

import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

/**
 * The comparison key of a value whose annotation fields the engine does not recognise are left
 * out, at every depth (decision 151). Such a field is another producer's, and takes no part in a
 * comparison key: two values that differ only in it compare equal. The annotations the engine
 * recognises stay, because the value determines them. The key is planned from the resolved type.
 */
object StripAnnotations {

  /** The prefix of the name of an annotation field (decision 152). */
  private val AnnotationPrefix = "__"

  /**
   * Returns a type without the annotation fields that are not recognised, at every depth.
   *
   * @param dataType   the type
   * @param recognised the names of the annotation fields the engine recognises
   * @return the type without the others
   */
  def stripped(dataType: DataType, recognised: Seq[String]): DataType = dataType match {
    case struct: StructType =>
      StructType(struct.fields
        .filterNot(field => field.name.startsWith(AnnotationPrefix)
          && !recognised.contains(field.name))
        .map(field => field.copy(dataType = stripped(field.dataType, recognised))))
    case ArrayType(element, containsNull) => ArrayType(stripped(element, recognised), containsNull)
    case other => other
  }
}
