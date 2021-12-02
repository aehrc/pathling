package au.csiro.pathling.encoders2

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.encoders2.SchemaTraversal.isCollection
import ca.uhn.fhir.context._


/**
 * Default implementation of [[SchemaProcessor]] delegating relevant functionality to [[DataTypeMappings]]
 *
 * @tparam DT the type which represents the final result of traversing a resource (or composite), e.g: for a schema converter this can be [[org.apache.spark.sql.types.DataType]].
 * @tparam SF the type which represents the result of traversing an element of a composite, e.g: for a schema converter this can be [[org.apache.spark.sql.types.StructField]].
 */
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


  /**
   * Builds the representation of a singular element.
   *
   * @param childDefinition   the element child definition.
   * @param elementDefinition the element definition.
   * @param elementName       the element name.
   * @param compositeBuilder  the callback to build the representation of a composite.
   * @return the representation of the singular element.
   */
  def buildSimpleValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                       compositeBuilder: (SchemaProcessor[DT, SF], BaseRuntimeElementCompositeDefinition[_]) => DT): DT = {
    childDefinition match {
      case enumChildDefinition: RuntimeChildPrimitiveEnumerationDatatypeDefinition =>
        buildEnumPrimitive(elementDefinition.asInstanceOf[RuntimePrimitiveDatatypeDefinition],
          enumChildDefinition)
      case _ =>
        elementDefinition match {
          case composite: BaseRuntimeElementCompositeDefinition[_] => compositeBuilder(this, composite)
          case primitive: RuntimePrimitiveDatatypeDefinition => buildPrimitiveDatatype(primitive)
          case xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org)
          case _: RuntimePrimitiveDatatypeNarrativeDefinition => buildPrimitiveDatatypeNarrative
        }
    }
  }

  /**
   * Builds the representation of a collection element.
   *
   * @param childDefinition   the element child definition.
   * @param elementDefinition the element definition.
   * @param elementName       the element name.
   * @param compositeBuilder  the callback to build the representation of a composite.
   * @return the representation of a collection element.
   */
  def buildArrayValue(childDefinition: BaseRuntimeChildDefinition, elementDefinition: BaseRuntimeElementDefinition[_], elementName: String,
                      compositeBuilder: (SchemaProcessor[DT, SF], BaseRuntimeElementCompositeDefinition[_]) => DT): DT

  /**
   * Builds the representation of a named element.
   *
   * @param elementName  the name of the element.
   * @param elementValue the representation of the element value.
   * @param definition   the element definition.
   * @return the representation of the named element.
   */
  def buildElement(elementName: String, elementValue: DT, definition: BaseRuntimeElementDefinition[_]): SF

  /**
   * Builds the representation of a primitive data type.
   *
   * @param primitive the primitive data type definition.
   * @return the primitive representation.
   */
  def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): DT

  /**
   * Builds the representation of a Narrative primitive.
   *
   * @return the representation for the primitive.
   */
  def buildPrimitiveDatatypeNarrative: DT

  /**
   * Builds the representation of a xhtmlHl7Org primitive.
   *
   * @param xhtmlHl7Org the definition of the xhtmlHl7Org primitive
   * @return the representation for the primitive.
   */
  def buildPrimitiveDatatypeXhtmlHl7Org(xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition): DT

  /**
   * Builds the representation of an Enum primitive.
   *
   * @param enumDefinition the enum child definition.
   * @param the enum element definition.
   * @return the representation for the primitive.
   */
  def buildEnumPrimitive(enumDefinition: RuntimePrimitiveDatatypeDefinition,
                         enumChildDefinition: RuntimeChildPrimitiveEnumerationDatatypeDefinition): DT = {
    buildPrimitiveDatatype(enumDefinition)
  }
}