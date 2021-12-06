/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.SchemaConverter
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders2.ExtensionSupport.{EXTENSIONS_FIELD_NAME, FID_FIELD_NAME}
import au.csiro.pathling.encoders2.SchemaVisitor.isCollection
import ca.uhn.fhir.context._
import org.apache.spark.sql.types._
import org.hl7.fhir.instance.model.api.IBase

/**
 * The schema processor for converting FHIR schemas to SQL schemas.
 *
 * @param fhirContext      the FHIR context to use.
 * @param dataTypeMappings data type mappings to use.
 */
private[encoders2] class SchemaConverterProcessor(override val fhirContext: FhirContext, val dataTypeMappings: DataTypeMappings,
                                                  override val maxNestingLevel: Int, override val expandExtensions: Boolean) extends
  SchemaProcessorWithTypeMappings[DataType, StructField] {


  private def createExtensionFields(definition: BaseRuntimeElementCompositeDefinition[_]): Seq[StructField] = {

    // for each composite add _fid and for resources also add _extension
    val maybeExtensionValueField = definition match {
      case _: RuntimeResourceDefinition =>
        val extensionSchema = buildExtensionValue()
        StructField(EXTENSIONS_FIELD_NAME, MapType(IntegerType, extensionSchema, valueContainsNull = false)) :: Nil
      case _ => Nil
    }
    // Add _fid field
    StructField(FID_FIELD_NAME, IntegerType) :: maybeExtensionValueField
  }

  override def buildComposite(definition: BaseRuntimeElementCompositeDefinition[_], fields: Seq[StructField]): DataType = {
    if (expandExtensions) {
      StructType(fields ++ createExtensionFields(definition))
    } else {
      StructType(fields)
    }
  }

  override def buildElement(elementName: String, elementValue: DataType, elementDefinition: BaseRuntimeElementDefinition[_]): StructField = {
    StructField(elementName, elementValue)
  }

  override def buildArrayValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String): DataType = {
    ArrayType(buildSimpleValue(childDefinition, elementDefinition, elementName))
  }

  override def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): DataType = {
    dataTypeMappings.primitiveToDataType(primitive)
  }

  override def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): DataType = DataTypes.StringType

  override def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String): Seq[StructField] = {
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    customEncoder.map(_.schema2(if (isCollection(childDefinition)) Some(ArrayType(_)) else None)).getOrElse(
      childDefinition match {
        // case _: RuntimeChildExtension => Nil
        case _ => super.buildValue(childDefinition, elementDefinition, elementName)
      }
    )
  }
}

/**
 * The converter from FHIR schemas to SQL schemas.
 *
 * @param fhirContext        the FHIR context to use.
 * @param dataTypeMappings   the data type mappings to use.
 * @param maxNestingLevel    the max nesting level to use.
 * @param supportsExtensions if the scheme should include extension representation.
 */
class SchemaConverter2(val fhirContext: FhirContext, val dataTypeMappings: DataTypeMappings, val maxNestingLevel: Int, val supportsExtensions: Boolean) extends SchemaConverter {

  private[encoders2] def compositeSchema(compositeElementDefinition: BaseRuntimeElementCompositeDefinition[_ <: IBase]): DataType = {
    SchemaVisitor.traverseComposite(compositeElementDefinition, new SchemaConverterProcessor(fhirContext, dataTypeMappings, maxNestingLevel, supportsExtensions))
  }

  override def resourceSchema(resourceDefinition: RuntimeResourceDefinition): StructType = {
    SchemaVisitor.traverseResource(resourceDefinition, new SchemaConverterProcessor(fhirContext, dataTypeMappings, maxNestingLevel, supportsExtensions)).asInstanceOf[StructType]
  }
}