/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific
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
 *
 */

package au.csiro.pathling.encoders.datatypes

import au.csiro.pathling.encoders.EncoderUtils.arrayExpression
import au.csiro.pathling.encoders.{Catalyst, ExpressionWithName}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, MapObjects, NewInstance}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.hl7.fhir.r4.model.IdType


/**
 * Custom coder for IdType.
 * Represents IdType on two dataset columns:
 * 'id' column hold the abbreviated unversioned id.
 * 'id_versioned' column holds the full id with version, type etc.
 *
 * @param elementName the name of the element.
 */
case class IdCustomCoder(elementName: String) extends CustomCoder {

  def primitiveClass: Class[IdType] = classOf[IdType]

  val versionedName: String = elementName + "_versioned"

  override def customDeserializer(addToPath: String => Expression,
                                  isCollection: Boolean): Seq[ExpressionWithName] = {

    // We can ignore the value in the `id` column and only deserialize from `id_versioned`
    def toVersionedId(exp: Expression): Expression = {
      NewInstance(primitiveClass,
        Invoke(exp, "toString", ObjectType(classOf[String])) :: Nil,
        ObjectType(primitiveClass))
    }

    val deserializerExp = if (!isCollection) {
      toVersionedId(addToPath(versionedName))
    } else {
      val array = Invoke(
        MapObjects(toVersionedId,
          addToPath(versionedName),
          StringType),
        "array",
        ObjectType(classOf[Array[Any]]))
      arrayExpression(array)
    }
    Seq((elementName, deserializerExp))
  }

  override def customSerializer(evaluator: (Expression => Expression) => Expression): Seq[ExpressionWithName] = {
    val idExpression = evaluator(
      exp => Catalyst.staticInvoke(classOf[UTF8String], DataTypes.StringType, "fromString",
        List(Invoke(exp, "getIdPart", ObjectType(classOf[String])))))

    val versionedIdExpression = evaluator(
      exp => Catalyst.staticInvoke(classOf[UTF8String], DataTypes.StringType, "fromString",
        List(Invoke(exp, "getValue", ObjectType(classOf[String])))))
    Seq((elementName, idExpression), (versionedName, versionedIdExpression))
  }

  override def schema(arrayEncoder: Option[DataType => DataType]): Seq[StructField] = {
    def encode(v: DataType): DataType = {
      arrayEncoder.map(_(v)).getOrElse(v)
    }

    Seq(StructField(elementName, encode(DataTypes.StringType)),
      StructField(versionedName, encode(DataTypes.StringType)))
  }
}
