package au.csiro.pathling.encoders2

import ca.uhn.fhir.context.{BaseRuntimeChildDefinition, BaseRuntimeElementDefinition, FhirContext, RuntimePrimitiveDatatypeDefinition}
import org.apache.spark.sql.types._

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

  override def buildArrayTransformer(arrayDefinition: BaseRuntimeChildDefinition): (Unit, BaseRuntimeElementDefinition[_]) => DataType = {
    // TODO: Should be able to use function composition here
    (ctx, elementDefinition) => {
      ArrayType(visitElementValue(ctx, elementDefinition))
    }
  }

  override def buildPrimitiveDatatypeNarrative: DataType = DataTypes.StringType

  override def buildPrimitiveDatatypeXhtmlHl7Org: DataType = DataTypes.StringType

}
