package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders2.SchemaTraversal.isCollection
import ca.uhn.fhir.context.{BaseRuntimeChildDefinition, BaseRuntimeElementCompositeDefinition, BaseRuntimeElementDefinition, RuntimeChildPrimitiveEnumerationDatatypeDefinition, RuntimePrimitiveDatatypeDefinition, RuntimePrimitiveDatatypeNarrativeDefinition, RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition}

trait SchemaProcessorWithTypeMappings[DT, SF] extends SchemaProcessor[DT, SF] {
  def dataTypeMappings: DataTypeMappings

  override def shouldExpandChild(definition: BaseRuntimeElementCompositeDefinition[_], childDefinition: BaseRuntimeChildDefinition): Boolean = {
    !dataTypeMappings.skipField(definition, childDefinition)
  }

  override def buildValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                          compositeBuilder: (SchemaProcessor[DT, SF], BaseRuntimeElementCompositeDefinition[_]) => DT): Seq[SF] = {
    val value = if (isCollection(childDefinition)) {
      buildArrayValue(childDefinition, elementDefinition, elementName, compositeBuilder)
    } else {
      buildSimpleValue(childDefinition, elementDefinition, elementName, compositeBuilder)
    }
    Seq(buildElement(elementName, value, elementDefinition))
  }


  def buildSimpleValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                       compositeBuilder: (SchemaProcessor[DT, SF], BaseRuntimeElementCompositeDefinition[_]) => DT): DT = {
    if (childDefinition.isInstanceOf[RuntimeChildPrimitiveEnumerationDatatypeDefinition]) {
      buildEnumDatatype(elementDefinition.asInstanceOf[RuntimePrimitiveDatatypeDefinition],
        childDefinition.asInstanceOf[RuntimeChildPrimitiveEnumerationDatatypeDefinition])
    } else {
      elementDefinition match {
        case composite: BaseRuntimeElementCompositeDefinition[_] => compositeBuilder(this, composite)
        case primitive: RuntimePrimitiveDatatypeDefinition => buildPrimitiveDatatype(primitive)
        case xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org)
        case _: RuntimePrimitiveDatatypeNarrativeDefinition => buildPrimitiveDatatypeNarrative
      }
    }
  }

  def buildArrayValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                      compositeBuilder: (SchemaProcessor[DT, SF], BaseRuntimeElementCompositeDefinition[_]) => DT): DT

  def buildElement(elementName: String, elementType: DT, definition: BaseRuntimeElementDefinition[_]): SF

  def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): DT

  def buildPrimitiveDatatypeNarrative: DT

  def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): DT

  def buildEnumDatatype(enumDefinition: RuntimePrimitiveDatatypeDefinition,
                        enumChildDefinition: RuntimeChildPrimitiveEnumerationDatatypeDefinition): DT = {
    buildPrimitiveDatatype(enumDefinition)
  }
}