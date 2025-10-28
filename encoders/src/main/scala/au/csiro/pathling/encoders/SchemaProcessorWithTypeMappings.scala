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

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import au.csiro.pathling.schema.ElementCtx
import au.csiro.pathling.schema.SchemaVisitor.isCollection
import ca.uhn.fhir.context._
import org.hl7.fhir.instance.model.api.IBase


/**
 * Default implementation of [[SchemaProcessor]] delegating relevant functionality to [[DataTypeMappings]]
 *
 * @tparam DT the type which represents the final result of traversing a resource (or composite), e.g: for a schema converter this can be [[org.apache.spark.sql.types.DataType]].
 * @tparam SF the type which represents the result of traversing an element of a composite, e.g: for a schema converter this can be [[org.apache.spark.sql.types.StructField]].
 */
abstract class SchemaProcessorWithTypeMappings[DT, SF]
  extends SchemaProcessor[DT, SF] with EncoderContext {


  private def isAllowedOpenElementType(cls: Class[_ <: IBase]): Boolean = {
    config.openTypes.contains(cls.getConstructor().newInstance().fhirType())
  }

  override def getValidChoiceTypes(choice: RuntimeChildChoiceDefinition): Seq[Class[_ <: IBase]] = {
    // delegate to type mappings and filter additionally restrict with config for RuntimeChildAny
    choice match {
      case _: RuntimeChildAny => dataTypeMappings.getValidChoiceTypes(choice)
        .filter(isAllowedOpenElementType)
      case _ => dataTypeMappings.getValidChoiceTypes(choice)
    }
  }

  override def shouldExpandChild(definition: BaseRuntimeElementCompositeDefinition[_],
                                 childDefinition: BaseRuntimeChildDefinition): Boolean = {

    // Do not expand extensions, as they require custom handling.
    val expandExtension = !childDefinition.isInstanceOf[RuntimeChildExtension]
    expandExtension && !dataTypeMappings.skipField(definition, childDefinition)
  }

  override def buildValue(childDefinition: BaseRuntimeChildDefinition,
                          elementDefinition: BaseRuntimeElementDefinition[_],
                          elementName: String): Seq[SF] = {
    val value = if (isCollection(childDefinition)) {
      buildArrayValue(childDefinition, elementDefinition, elementName)
    } else {
      buildSimpleValue(childDefinition, elementDefinition, elementName)
    }
    Seq(buildElement(elementName, value, elementDefinition))
  }

  /**
   * Builds the representation of a singular element.
   *
   * @param childDefinition   the element child definition.
   * @param elementDefinition the element definition.
   * @param elementName       the element name.
   * @return the representation of the singular element.
   */
  def buildSimpleValue(childDefinition: BaseRuntimeChildDefinition,
                       elementDefinition: BaseRuntimeElementDefinition[_],
                       elementName: String): DT = {
    childDefinition match {
      // Enumerations require special processing because the information necessary to determine
      // the actual EnumFactory type required to construct a complete Enumeration instance
      // is available  in RuntimeChildPrimitiveEnumerationDatatypeDefinition,
      // but not in RuntimePrimitiveDatatypeDefinition.
      case enumChildDefinition: RuntimeChildPrimitiveEnumerationDatatypeDefinition =>
        buildEnumPrimitive(elementDefinition.asInstanceOf[RuntimePrimitiveDatatypeDefinition],
          enumChildDefinition)
      case _ =>
        elementDefinition match {
          case composite: BaseRuntimeElementCompositeDefinition[_] => compositeBuilder(composite)
          case primitive: RuntimePrimitiveDatatypeDefinition => buildPrimitiveDatatype(primitive)
          case xhtmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => buildPrimitiveDatatypeXhtmlHl7Org(
            xhtmlHl7Org)
          case _ => throw new IllegalArgumentException(
            "Cannot process element: " + elementName + " with definition: " + elementDefinition)
        }
    }
  }

  /**
   * Builds the representation of a collection element.
   *
   * @param childDefinition   the element child definition.
   * @param elementDefinition the element definition.
   * @param elementName       the element name.
   * @return the representation of a collection element.
   */
  def buildArrayValue(childDefinition: BaseRuntimeChildDefinition,
                      elementDefinition: BaseRuntimeElementDefinition[_], elementName: String): DT

  /**
   * Builds the representation of a named element.
   *
   * @param elementName  the name of the element.
   * @param elementValue the representation of the element value.
   * @param definition   the element definition.
   * @return the representation of the named element.
   */
  def buildElement(elementName: String, elementValue: DT,
                   definition: BaseRuntimeElementDefinition[_]): SF

  /**
   * Builds the representation of a primitive data type.
   *
   * @param primitive the primitive data type definition.
   * @return the primitive representation.
   */
  def buildPrimitiveDatatype(primitive: RuntimePrimitiveDatatypeDefinition): DT

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
   * @param enumDefinition      the enum child definition.
   * @param enumChildDefinition the enum element definition.
   * @return the representation for the primitive.
   */
  def buildEnumPrimitive(enumDefinition: RuntimePrimitiveDatatypeDefinition,
                         enumChildDefinition: RuntimeChildPrimitiveEnumerationDatatypeDefinition): DT = {
    buildPrimitiveDatatype(enumDefinition)
  }


  /**
   * Builds the representation of Extension element.
   *
   * @return the representation of Extension element.
   */
  def buildExtensionValue(): DT = {
    val extensionNode = ElementCtx.forExtension(fhirContext)
    buildArrayValue(extensionNode.childDefinition, extensionNode.elementDefinition,
      extensionNode.elementName)
  }
}
