package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.SchemaConverter
import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import ca.uhn.fhir.context._
import org.apache.spark.sql.types._
import org.hl7.fhir.instance.model.api.IBase

//class SchemaConverter2(val fhirContext: FhirContext, val dataTypeMappings: DataTypeMappings, val maxNestingLevel: Int) extends
//  SchemaTraversal[DataType, StructField, Unit](fhirContext, maxNestingLevel) with SchemaConverter {
//
//  override def buildComposite(ctx: Unit, fields: Seq[StructField], definition: BaseRuntimeElementCompositeDefinition[_]): DataType = {
//    StructType(fields)
//  }
//
//  override def buildPrimitiveDatatype(ctx: Unit, primitive: RuntimePrimitiveDatatypeDefinition): DataType = {
//    //println(s"visitElement: ${primitive}]")
//    dataTypeMappings.primitiveToDataType(primitive)
//  }
//
//  override def buildElement(elementName: String, elementType: DataType, elementDefinition: BaseRuntimeElementDefinition[_]): StructField = {
//    StructField(elementName, elementType)
//  }
//
//  override def buildArrayTransformer(arrayDefinition: BaseRuntimeChildDefinition): (Unit, BaseRuntimeElementDefinition[_]) => DataType = {
//    // TODO: Should be able to use function composition here
//    (ctx, elementDefinition) => {
//      ArrayType(visitElementValue(ctx, elementDefinition, arrayDefinition))
//    }
//  }
//
//  override def buildPrimitiveDatatypeNarrative(ctx: Unit): DataType = DataTypes.StringType
//
//  override def buildPrimitiveDatatypeXhtmlHl7Org(ctx: Unit, xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): DataType = DataTypes.StringType
//
//  override def shouldExpandChild(definition: BaseRuntimeElementCompositeDefinition[_], childDefinition: BaseRuntimeChildDefinition): Boolean = {
//    // TODO: This should be unified somewhere else
//    !dataTypeMappings.skipField(definition, childDefinition)
//  }
//
//  override def buildValue(ctx: Unit, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String, valueBuilder: (Unit, BaseRuntimeElementDefinition[_]) => DataType): Seq[StructField] = {
//    // add custom encoder
//    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
//    // TODO: Enable this check or implement
//    //assert(customEncoder.isEmpty || !isCollection,
//    //"Collections are not supported for custom encoders for: " + elementName + "-> " + elementDefinition)
//    customEncoder.map(_.schema).getOrElse(super.buildValue(ctx, elementDefinition, elementName, valueBuilder))
//  }
//
//  def resourceSchema(resourceDefinition: RuntimeResourceDefinition): StructType = {
//    enterResource(null, resourceDefinition).asInstanceOf[StructType]
//  }
//
//}


class SchemaConverterVisitor(val fhirContext: FhirContext, val dataTypeMappings: DataTypeMappings) extends
  SchemaVisitorWithTypeMappings[DataType, StructField] {

  override def buildComposite(fields: Seq[StructField], definition: BaseRuntimeElementCompositeDefinition[_]): DataType = {
    StructType(fields)
  }

  override def buildElement(elementName: String, elementType: DataType, elementDefinition: BaseRuntimeElementDefinition[_]): StructField = {
    StructField(elementName, elementType)
  }

  override def buildArrayValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                               compositeBuilder: (SchemaVisitorEx[DataType, StructField], BaseRuntimeElementCompositeDefinition[_]) => DataType): DataType = {
    ArrayType(buildSimpleValue(childDefinition, elementDefinition, elementName, compositeBuilder))
  }

  override def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): DataType = {
    dataTypeMappings.primitiveToDataType(primitive)
  }

  override def buildPrimitiveDatatypeNarrative: DataType = DataTypes.StringType

  override def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): DataType = DataTypes.StringType

  override def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                          compositeBuilder: (SchemaVisitorEx[DataType, StructField], BaseRuntimeElementCompositeDefinition[_]) => DataType): Seq[StructField] = {
    // add custom encoder
    val customEncoder = dataTypeMappings.customEncoder(elementDefinition, elementName)
    // TODO: Enable this check or implement
    //assert(customEncoder.isEmpty || !isCollection,
    //"Collections are not supported for custom encoders for: " + elementName + "-> " + elementDefinition)
    customEncoder.map(_.schema).getOrElse(
      super.buildValue(childDefinition, elementDefinition, elementName, compositeBuilder)
    )
  }

}

class SchemaConverter2(val fhirContext: FhirContext, val dataTypeMappings: DataTypeMappings, val maxNestingLevel: Int) extends SchemaConverter {

  private[encoders2] def compositeSchema(compositeElementDefinition: BaseRuntimeElementCompositeDefinition[_ <: IBase]): DataType = {
    // TODO: unify the traversal
    new SchemaTraversalEx[DataType, StructField](maxNestingLevel)
      .enterComposite(new SchemaConverterVisitor(fhirContext, dataTypeMappings), compositeElementDefinition)
  }

  override def resourceSchema(resourceDefinition: RuntimeResourceDefinition): StructType = {
    new SchemaTraversalEx[DataType, StructField](maxNestingLevel)
      .enterResource(new SchemaConverterVisitor(fhirContext, dataTypeMappings), resourceDefinition).asInstanceOf[StructType]
  }
}