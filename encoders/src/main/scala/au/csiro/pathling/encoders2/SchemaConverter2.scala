package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.SchemaConverter
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders2.SchemaTraversal.isCollection
import ca.uhn.fhir.context._
import org.apache.spark.sql.types._
import org.hl7.fhir.instance.model.api.IBase

/**
 * The schema processor for converting FHIR schemas to SQL schemas.
 *
 * @param fhirContext      the FHIR context to use.
 * @param dataTypeMappings data type mappings to use.
 */
private[encoders2] class SchemaConverterProcessor(val fhirContext: FhirContext, val dataTypeMappings: DataTypeMappings) extends
  SchemaProcessorWithTypeMappings[DataType, StructField] {

  override def buildComposite(definition: BaseRuntimeElementCompositeDefinition[_], fields: Seq[StructField]): DataType = {
    StructType(fields)
  }

  override def buildElement(elementName: String, elementValue: DataType, elementDefinition: BaseRuntimeElementDefinition[_]): StructField = {
    StructField(elementName, elementValue)
  }

  override def buildArrayValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                               compositeBuilder: (SchemaProcessor[DataType, StructField], BaseRuntimeElementCompositeDefinition[_]) => DataType): DataType = {
    ArrayType(buildSimpleValue(childDefinition, elementDefinition, elementName, compositeBuilder))
  }

  override def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): DataType = {
    dataTypeMappings.primitiveToDataType(primitive)
  }

  override def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): DataType = DataTypes.StringType

  override def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                          compositeBuilder: (SchemaProcessor[DataType, StructField], BaseRuntimeElementCompositeDefinition[_]) => DataType): Seq[StructField] = {
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    customEncoder.map(_.schema2(if (isCollection(childDefinition)) Some(ArrayType(_)) else None)).getOrElse(
      super.buildValue(childDefinition, elementDefinition, elementName, compositeBuilder)
    )
  }

}

/**
 * The converter from FHIR schemas to SQL schemas.
 *
 * @param fhirContext      the FHIR context to use.
 * @param dataTypeMappings the data type mappings to use.
 * @param maxNestingLevel  the max nesting level to use.
 */
class SchemaConverter2(val fhirContext: FhirContext, val dataTypeMappings: DataTypeMappings, val maxNestingLevel: Int) extends SchemaConverter {

  private[encoders2] def compositeSchema(compositeElementDefinition: BaseRuntimeElementCompositeDefinition[_ <: IBase]): DataType = {
    new SchemaTraversal[DataType, StructField](maxNestingLevel)
      .processComposite(new SchemaConverterProcessor(fhirContext, dataTypeMappings), compositeElementDefinition)
  }

  override def resourceSchema(resourceDefinition: RuntimeResourceDefinition): StructType = {
    new SchemaTraversal[DataType, StructField](maxNestingLevel)
      .processResource(new SchemaConverterProcessor(fhirContext, dataTypeMappings), resourceDefinition).asInstanceOf[StructType]
  }
}