package au.csiro.pathling.encoders2

import ca.uhn.fhir.context.{BaseRuntimeElementDefinition, FhirContext, RuntimePrimitiveDatatypeDefinition}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

class SchemaConverter2(fhirContext: FhirContext, maxNestingLevel: Int) extends
  SchemaTraversal[DataType, StructField, Unit](fhirContext, maxNestingLevel) {

  override def buildComposite(ctx: Unit, fields: Seq[StructField]): DataType = {
    StructType(fields)
  }

  override def buildPrimitiveDatatype(ctx: Unit, primitive: RuntimePrimitiveDatatypeDefinition): DataType = {
    println(s"visitElement: ${primitive}]")
    // dataTypeMappings.primitiveToDataType(primitive)
    DataTypes.StringType
  }

  override def buildElement(elementName: String, elementType: DataType, elementDefinition: BaseRuntimeElementDefinition[_]): StructField = {
    StructField(elementName, elementType)
  }

  override def buildPrimitiveDatatypeNarrative: DataType = DataTypes.StringType

  override def buildPrimitiveDatatypeXhtmlHl7Org: DataType = DataTypes.StringType

}
