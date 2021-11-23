package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import ca.uhn.fhir.context._
import org.apache.spark.sql.types._
import org.hl7.fhir.instance.model.api.IBaseResource

class SchemaConverter2(fhirContext: FhirContext, dataTypeMappings: DataTypeMappings, maxNestingLevel: Int) extends
  SchemaTraversal[DataType, StructField, Unit](fhirContext, maxNestingLevel) {

  override def buildComposite(ctx: Unit, fields: Seq[StructField], definition: BaseRuntimeElementCompositeDefinition[_]): DataType = {
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
      ArrayType(visitElementValue(ctx, elementDefinition, arrayDefinition))
    }
  }

  override def buildPrimitiveDatatypeNarrative(ctx: Unit): DataType = DataTypes.StringType

  override def buildPrimitiveDatatypeXhtmlHl7Org(ctx: Unit, xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): DataType = DataTypes.StringType

  override def shouldExpandChild(definition: BaseRuntimeElementCompositeDefinition[_], childDefinition: BaseRuntimeChildDefinition): Boolean = {
    // TODO: This should be unified somewhere else
    !dataTypeMappings.skipField(definition, childDefinition)
  }

  override def buildValue(ctx: Unit, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String, valueBuilder: (Unit, BaseRuntimeElementDefinition[_]) => DataType): Seq[StructField] = {
    // add custom encoder
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    // TODO: Enable this check or implement
    //assert(customEncoder.isEmpty || !isCollection,
    //"Collections are not supported for custom encoders for: " + elementName + "-> " + elementDefinition)
    customEncoder.map(_.schema).getOrElse(super.buildValue(ctx, elementDefinition, elementName, valueBuilder))
  }

  def resourceSchema[T <: IBaseResource](resourceClass: Class[T]): StructType = {
    resourceSchema(fhirContext.getResourceDefinition(resourceClass))
  }

  def resourceSchema(resourceDefinition: RuntimeResourceDefinition): StructType = {
    enterResource(null, resourceDefinition).asInstanceOf[StructType]
  }

}
