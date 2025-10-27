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

import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings.{fhirPrimitiveToSparkTypes, isValidOpenElementType}
import au.csiro.pathling.encoders.{Catalyst, ExpressionWithName, StaticField}
import ca.uhn.fhir.context._
import ca.uhn.fhir.model.api.TemporalPrecisionEnum
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.expressions.objects.{InitializeJavaBean, Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.types.{DataType, DataTypes, ObjectType}
import org.hl7.fhir.instance.model.api.{IBase, IBaseDatatype, IPrimitiveType}
import org.hl7.fhir.r4.model._

import java.util.TimeZone
import scala.jdk.CollectionConverters._

/**
 * Data type mappings for FHIR STU3.
 */
class R4DataTypeMappings extends DataTypeMappings {

  override def primitiveToDataType(definition: RuntimePrimitiveDatatypeDefinition): DataType = {

    fhirPrimitiveToSparkTypes.get(definition.getImplementingClass) match {

      case Some(dataType) => dataType
      case None => throw new IllegalArgumentException(
        "Unknown primitive type: " + definition.getImplementingClass.getName)
    }
  }

  override def baseType(): Class[_ <: IBaseDatatype] = classOf[org.hl7.fhir.r4.model.Type]

  override def overrideCompositeExpression(inputObject: Expression,
                                           definition: BaseRuntimeElementCompositeDefinition[_]): Option[Seq[ExpressionWithName]] = {
    None
  }

  override def skipField(definition: BaseRuntimeElementCompositeDefinition[_],
                         child: BaseRuntimeChildDefinition): Boolean = {
    // Contains elements are currently not encoded in our Spark dataset.
    val skipContains = definition
      .getImplementingClass == classOf[ValueSet.ValueSetExpansionContainsComponent] &&
      child.getElementName == "contains"

    // This is due to a bug in HAPI RuntimeChildExtension.getChildByName() 
    // implementation, which fails on assertion because the name of the child 
    // should be "modifierExtensionExtension", not "extensionExtension". 
    // See: https://github.com/hapifhir/hapi-fhir/issues/3414
    val skipModifierExtension = child.getElementName.equals("modifierExtension")
    skipContains || skipModifierExtension
  }

  override def primitiveEncoderExpression(inputObject: Expression,
                                          primitive: RuntimePrimitiveDatatypeDefinition): Expression = {

    primitive.getImplementingClass match {

      // If the FHIR primitive is serialized as a string, convert it to UTF8.
      case cls if fhirPrimitiveToSparkTypes.get(cls).contains(DataTypes.StringType) =>
        dataTypeToUtf8Expr(inputObject)

      case boolClass if boolClass == classOf[org.hl7.fhir.r4.model.BooleanType] =>
        Invoke(inputObject, "getValue", DataTypes.BooleanType)

      case tsClass if tsClass == classOf[org.hl7.fhir.r4.model.InstantType] =>

        // NOTE: using the instantiation with the new operator is necessary to invoke the auxiliary
        // constructor that is common between version 3.3 and 3.4 of spark catalyst.
        // The 3.4 code is deployed in databricks runtimes version >= 11.1.
        new Cast(dataTypeToUtf8Expr(inputObject), DataTypes.TimestampType, Some("UTC"))

      case base64Class if base64Class == classOf[org.hl7.fhir.r4.model.Base64BinaryType] =>

        Invoke(inputObject, "getValue", DataTypes.BinaryType)

      case intClass if intClass == classOf[org.hl7.fhir.r4.model.IntegerType] =>

        Invoke(inputObject, "getValue", DataTypes.IntegerType)

      case unsignedIntClass if unsignedIntClass == classOf[org.hl7.fhir.r4.model.UnsignedIntType] =>

        Invoke(inputObject, "getValue", DataTypes.IntegerType)

      case unsignedIntClass if unsignedIntClass == classOf[org.hl7.fhir.r4.model.PositiveIntType] =>

        Invoke(inputObject, "getValue", DataTypes.IntegerType)

      case unknown =>
        throw new IllegalArgumentException(
          "Cannot serialize unknown primitive type: " + unknown.getName)
    }
  }

  override def primitiveDecoderExpression(primitiveClass: Class[_ <: IPrimitiveType[_]],
                                          path: Option[Expression]): Expression = {

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(primitiveClass)))

    primitiveClass match {

      // If the FHIR primitive is represented as a string type, read it from UTF8 and
      // set the value.
      case _ if fhirPrimitiveToSparkTypes.get(primitiveClass).contains(DataTypes.StringType) =>

        val newInstance = NewInstance(primitiveClass,
          Nil,
          ObjectType(primitiveClass))

        // Convert UTF8String to a regular string.
        InitializeJavaBean(newInstance, Map("setValueAsString" ->
          Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)))

      // Classes that can be directly encoded as their primitive type.
      case cls if cls == classOf[org.hl7.fhir.r4.model.BooleanType] ||
        cls == classOf[org.hl7.fhir.r4.model.Base64BinaryType] ||
        cls == classOf[org.hl7.fhir.r4.model.IntegerType] ||
        cls == classOf[org.hl7.fhir.r4.model.UnsignedIntType] ||
        cls == classOf[org.hl7.fhir.r4.model.PositiveIntType] =>
        NewInstance(primitiveClass,
          List(getPath),
          ObjectType(primitiveClass))

      case instantClass if instantClass == classOf[org.hl7.fhir.r4.model.InstantType] =>

        val millis = StaticField(classOf[TemporalPrecisionEnum],
          ObjectType(classOf[TemporalPrecisionEnum]),
          "MILLI")

        val UTCZone = Catalyst.staticInvoke(classOf[TimeZone],
          ObjectType(classOf[TimeZone]),
          "getTimeZone",
          Literal("UTC", ObjectType(classOf[String])) :: Nil)

        NewInstance(primitiveClass,
          List(Catalyst.staticInvoke(org.apache.spark.sql.catalyst.util.DateTimeUtils.getClass,
            ObjectType(classOf[java.sql.Timestamp]),
            "toJavaTimestamp",
            getPath :: Nil),
            millis,
            UTCZone),
          ObjectType(primitiveClass))

      case unknown => throw new IllegalArgumentException(
        "Cannot deserialize unknown primitive type: " + unknown.getName)
    }
  }

  override def customEncoder(elementDefinition: BaseRuntimeElementDefinition[_],
                             elementName: String): Option[CustomCoder] = {
    elementDefinition match {
      case primitive: RuntimePrimitiveDatatypeDefinition
        if classOf[org.hl7.fhir.r4.model.DecimalType] == primitive.getImplementingClass =>
        Some(DecimalCustomCoder(elementName))
      case primitive: RuntimePrimitiveDatatypeDefinition
        if classOf[org.hl7.fhir.r4.model.IdType] == primitive.getImplementingClass =>
        Some(IdCustomCoder(elementName))
      case _ => super.customEncoder(elementDefinition, elementName)
    }
  }


  override def getValidChoiceTypes(choice: RuntimeChildChoiceDefinition): Seq[Class[_ <: IBase]] = {
    choice
      .getValidChildTypes.asScala
      .filter(cls => !choice.isInstanceOf[RuntimeChildAny] || isValidOpenElementType(cls))
      .toList
  }
}

/**
 * Companion object for R4DataTypeMappings
 */
object R4DataTypeMappings {
  /**
   * Map associating FHIR primitive datatypes with the Spark types used to encode them.
   */
  private val fhirPrimitiveToSparkTypes: Map[Class[_ <: IPrimitiveType[_]], DataType] =
    Map(
      classOf[MarkdownType] -> DataTypes.StringType,
      classOf[Enumeration[_]] -> DataTypes.StringType,
      classOf[DateTimeType] -> DataTypes.StringType,
      classOf[TimeType] -> DataTypes.StringType,
      classOf[DateType] -> DataTypes.StringType,
      classOf[CodeType] -> DataTypes.StringType,
      classOf[StringType] -> DataTypes.StringType,
      classOf[UriType] -> DataTypes.StringType,
      classOf[UrlType] -> DataTypes.StringType,
      classOf[CanonicalType] -> DataTypes.StringType,
      classOf[IntegerType] -> DataTypes.IntegerType,
      classOf[UnsignedIntType] -> DataTypes.IntegerType,
      classOf[PositiveIntType] -> DataTypes.IntegerType,
      classOf[BooleanType] -> DataTypes.BooleanType,
      classOf[InstantType] -> DataTypes.TimestampType,
      classOf[Base64BinaryType] -> DataTypes.BinaryType,
      classOf[OidType] -> DataTypes.StringType,
      classOf[UuidType] -> DataTypes.StringType
    )


  /**
   * Non primitive datatypes that are allowed in open choices like value[*].
   * As defined in:https://www.hl7.org/fhir/datatypes.html#open
   */
  private val allowedOpenTypes: Set[Class[_]] = Set(
    // DataTypes
    classOf[org.hl7.fhir.r4.model.Address],
    classOf[org.hl7.fhir.r4.model.Age],
    classOf[org.hl7.fhir.r4.model.Annotation],
    classOf[org.hl7.fhir.r4.model.Attachment],
    classOf[org.hl7.fhir.r4.model.CodeableConcept],
    classOf[org.hl7.fhir.r4.model.Coding],
    classOf[org.hl7.fhir.r4.model.ContactPoint],
    classOf[org.hl7.fhir.r4.model.Count],
    classOf[org.hl7.fhir.r4.model.Distance],
    classOf[org.hl7.fhir.r4.model.Duration],
    classOf[org.hl7.fhir.r4.model.HumanName],
    classOf[org.hl7.fhir.r4.model.Identifier],
    classOf[org.hl7.fhir.r4.model.Money],
    classOf[org.hl7.fhir.r4.model.Period],
    classOf[org.hl7.fhir.r4.model.Quantity],
    classOf[org.hl7.fhir.r4.model.Range],
    classOf[org.hl7.fhir.r4.model.Ratio],
    classOf[org.hl7.fhir.r4.model.Reference],
    classOf[org.hl7.fhir.r4.model.SampledData],
    classOf[org.hl7.fhir.r4.model.Signature],
    classOf[org.hl7.fhir.r4.model.Timing],
    // MetaDataTypes
    classOf[org.hl7.fhir.r4.model.ContactDetail],
    classOf[org.hl7.fhir.r4.model.Contributor],
    classOf[org.hl7.fhir.r4.model.DataRequirement],
    classOf[org.hl7.fhir.r4.model.Expression],
    classOf[org.hl7.fhir.r4.model.ParameterDefinition],
    classOf[org.hl7.fhir.r4.model.RelatedArtifact],
    classOf[org.hl7.fhir.r4.model.TriggerDefinition],
    classOf[org.hl7.fhir.r4.model.UsageContext],
    // Special Types
    classOf[org.hl7.fhir.r4.model.Dosage],
    classOf[org.hl7.fhir.r4.model.Meta]
  )


  /**
   * Checks if the given class is a valid type for open elements types as defined in: [[https://build.fhir.org/datatypes.html#open]].
   * Note: this is needed because the HAPI implementation of open type element [[ca.uhn.fhir.context.RuntimeChildAny#getValidChildTypes]] returns
   * types not included in the specification such as [[org.hl7.fhir.r4.model.ElementDefinition]].
   *
   * @param cls the class of the type to checks.
   * @return true is given type is a valid open element type.
   */
  def isValidOpenElementType(cls: Class[_ <: IBase]): Boolean = {
    classOf[PrimitiveType[_]].isAssignableFrom(cls) || allowedOpenTypes.contains(cls)
  }
}
