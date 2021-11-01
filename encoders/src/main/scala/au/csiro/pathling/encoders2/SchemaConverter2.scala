package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import ca.uhn.fhir.context._
import org.apache.spark.sql.types._
import org.hl7.fhir.instance.model.api.IBaseResource

class SchemaConverter2(fhirContext: FhirContext, dataTypeMappings: DataTypeMappings, maxNestingLevel: Int) extends
  SchemaTraversal[DataType, StructField, Unit](fhirContext, maxNestingLevel) {

  override def buildComposite(ctx: Unit, fields: Seq[StructField]): DataType = {
    StructType(fields)
  }

  override def buildPrimitiveDatatype(ctx: Unit, primitive: RuntimePrimitiveDatatypeDefinition): DataType = {
    //println(s"visitElement: ${primitive}]")
    dataTypeMappings.primitiveToDataType(primitive)
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

  override def shouldExpandChild(definition: BaseRuntimeElementCompositeDefinition[_], childDefinition: BaseRuntimeChildDefinition): Boolean = {
    // TODO: This should be unified somewhere else
    !dataTypeMappings.skipField(definition, childDefinition)
  }

  def resourceSchema[T <: IBaseResource](resourceClass: Class[T]): StructType = {
    visitResource(null, resourceClass).asInstanceOf[StructType]
  }
}
