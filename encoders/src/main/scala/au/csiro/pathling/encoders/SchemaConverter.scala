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

package au.csiro.pathling.encoders

import au.csiro.pathling.encoders.ExtensionSupport.{EXTENSIONS_FIELD_NAME, FID_FIELD_NAME}
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.schema.SchemaVisitor
import au.csiro.pathling.schema.SchemaVisitor.isCollection
import ca.uhn.fhir.context._
import org.apache.spark.sql.types._
import org.hl7.fhir.instance.model.api.{IBase, IBaseResource}
import org.hl7.fhir.r4.model.Quantity

/**
 * The schema processor for converting FHIR schemas to SQL schemas.
 *
 * @param fhirContext      the FHIR context to use.
 * @param dataTypeMappings data type mappings to use.
 * @param config           encoder configuration to use.
 */
private[encoders] class SchemaConverterProcessor(override val fhirContext: FhirContext,
                                                 override val dataTypeMappings: DataTypeMappings,
                                                 override val config: EncoderConfig) extends
  SchemaProcessorWithTypeMappings[DataType, StructField] {

  private def createExtensionField(definition: BaseRuntimeElementCompositeDefinition[_]): Seq[StructField] = {
    // For resources, also add _extension.
    definition match {
      case _: RuntimeResourceDefinition if supportsExtensions =>
        val extensionSchema = buildExtensionValue()
        StructField(EXTENSIONS_FIELD_NAME,
          MapType(IntegerType, extensionSchema, valueContainsNull = false)) :: Nil
      case _ => Nil
    }
  }

  private def createFidField(): Seq[StructField] = {
    // For each composite, add _fid.
    if (generateFid) {
      StructField(FID_FIELD_NAME, IntegerType) :: Nil
    } else {
      Nil
    }
  }

  private def createQuantityFields(definition: BaseRuntimeElementCompositeDefinition[_]): Seq[StructField] = {
    definition.getImplementingClass match {
      case cls if classOf[Quantity].isAssignableFrom(cls) => QuantitySupport
        .createExtraSchemaFields()
      case _ => Nil
    }
  }

  override def buildComposite(definition: BaseRuntimeElementCompositeDefinition[_],
                              fields: Seq[StructField]): DataType = {
    StructType(
      fields ++ createQuantityFields(definition) ++ createFidField() ++ createExtensionField(
        definition))
  }

  override def buildElement(elementName: String, elementValue: DataType,
                            elementDefinition: BaseRuntimeElementDefinition[_]): StructField = {
    StructField(elementName, elementValue)
  }

  override def buildArrayValue(childDefinition: BaseRuntimeChildDefinition,
                               elementDefinition: BaseRuntimeElementDefinition[_],
                               elementName: String): DataType = {
    ArrayType(buildSimpleValue(childDefinition, elementDefinition, elementName))
  }

  override def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): DataType = {
    dataTypeMappings.primitiveToDataType(primitive)
  }

  override def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): DataType = DataTypes
    .StringType

  override def buildValue(childDefinition: BaseRuntimeChildDefinition,
                          elementDefinition: BaseRuntimeElementDefinition[_],
                          elementName: String): Seq[StructField] = {
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    customEncoder.map(_.schema(if (isCollection(childDefinition)) Some(ArrayType(_)) else None))
      .getOrElse(super.buildValue(childDefinition, elementDefinition, elementName))
  }
}

/**
 * The converter from FHIR schemas to SQL schemas.
 *
 * @param fhirContext      the FHIR context to use.
 * @param dataTypeMappings the data type mappings to use.
 * @param config           encoder configuration to use.
 */
class SchemaConverter(val fhirContext: FhirContext, val dataTypeMappings: DataTypeMappings,
                      val config: EncoderConfig) extends EncoderContext {

  private[encoders] def compositeSchema(compositeElementDefinition: BaseRuntimeElementCompositeDefinition[_ <: IBase]): DataType = {
    SchemaVisitor.traverseComposite(compositeElementDefinition,
      new SchemaConverterProcessor(fhirContext, dataTypeMappings, config))
  }

  /**
   * Returns the (spark) SQL schema that represents the given FHIR resource definition.
   *
   * @param resourceDefinition the FHIR resource definition.
   * @return the schema as a Spark StructType
   */
  def resourceSchema(resourceDefinition: RuntimeResourceDefinition): StructType = {
    SchemaVisitor.traverseResource(resourceDefinition,
      new SchemaConverterProcessor(fhirContext, dataTypeMappings, config)).asInstanceOf[StructType]
  }

  /**
   * Returns the spark (SQL) schema that represents the given FHIR resource class.
   *
   * @param resourceClass The class implementing the FHIR resource.
   * @return The schema as a Spark StructType
   */
  def resourceSchema[T <: IBaseResource](resourceClass: Class[T]): StructType = {
    resourceSchema(fhirContext.getResourceDefinition(resourceClass))
  }
}
