/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
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

import au.csiro.pathling.encoders.{Catalyst, ExpressionWithName}
import ca.uhn.fhir.context._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.types.{DataType, DataTypes, ObjectType}
import org.apache.spark.unsafe.types.UTF8String
import org.hl7.fhir.instance.model.api.{IBase, IBaseDatatype, IPrimitiveType}

/**
 * Interface for mapping FHIR datatypes to Spark datatypes.
 */
trait DataTypeMappings {

  /**
   * Converts the given FHIR primitive to the Spark DataType used to encode it.
   *
   * @param definition the FHIR datatype definition.
   * @return the corresponding Spark datatype
   */
  def primitiveToDataType(definition: RuntimePrimitiveDatatypeDefinition): DataType

  /**
   * Returns the base class of FHIR types in the version used.
   *
   * @return the base class for FHIR types in the version used
   */
  def baseType(): Class[_ <: IBaseDatatype]

  /**
   * Returns a Spark expression that translate a string-based FHIR type to a Spark UTF8 string.
   * This pattern occurs frequently, so this method is here as a convenience.
   *
   * @param inputObject the expression referring to a string-based FHIR type
   * @return an expression that produces a Spark UTF8 string
   */
  def dataTypeToUtf8Expr(inputObject: Expression): Expression = {

    Catalyst.staticInvoke(
      classOf[UTF8String],
      DataTypes.StringType,
      "fromString",
      List(Invoke(inputObject,
        "getValueAsString",
        ObjectType(classOf[String]))))
  }

  /**
   * Allows custom expressions to be used when encoding composite objects. This supports
   * special cases where FHIR objects don't follow conventions expected by reusable
   * encoding logic, allowing custom expressions to be used just for that case.
   *
   * For most expressions this method should simply return None, indicate that no override
   * is necessary.
   *
   * @param inputObject an expression referring to the composite object to encode
   * @param definition  the composite definition to encode
   * @return an optional sequence of named expressions if the composite is overridden.
   */
  def overrideCompositeExpression(inputObject: Expression,
                                  definition: BaseRuntimeElementCompositeDefinition[_]): Option[Seq[ExpressionWithName]]


  /**
   * Returns true if the given field should be skipped during encoding and decoding, false otherwise.
   * This allows the data type to explicitly short circuit a handful of recursive data model
   * definitions that cannot be encoded in Spark.
   */
  def skipField(compositeDefinition: BaseRuntimeElementCompositeDefinition[_],
                child: BaseRuntimeChildDefinition): Boolean

  /**
   * Returns an expression to serialize a primitive type.
   */
  def primitiveEncoderExpression(inputObject: Expression,
                                 primitive: RuntimePrimitiveDatatypeDefinition): Expression

  /**
   * Returns an expression to deserialize a primitive type.
   */
  def primitiveDecoderExpression(primitiveClass: Class[_ <: IPrimitiveType[_]],
                                 path: Option[Expression]): Expression

  /**
   * Returns a specialized custom coder for this child definition.
   *
   * @param elementDefinition the element definition
   * @param elementName       the name of the element
   * @return a specialized custom coder
   */

  def customEncoder(elementDefinition: BaseRuntimeElementDefinition[_],
                    elementName: String): Option[CustomCoder] = None


  /**
   * Returns the list of valid child types of given choice.
   *
   * @param choice the choice child definition.
   * @return list of valid types for this
   */
  def getValidChoiceTypes(choice: RuntimeChildChoiceDefinition): Seq[Class[_ <: IBase]]
}
