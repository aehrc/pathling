package au.csiro.pathling.encoders2

import ca.uhn.fhir.context.{BaseRuntimeElementCompositeDefinition, FhirContext}
import org.hl7.fhir.r4.model.Patient


object ExtensionSupport {

  val FID_FIELD_NAME: String = "_fid"
  val EXTENSIONS_FIELD_NAME: String = "_extension"

  val EXTENSION_ELEMENT_NAME: String = "extension"

  def extension[DT, ST](fhirContext: FhirContext): ElementNode[DT, ST] = {
    val baseResourceDefinition = fhirContext.getResourceDefinition(classOf[Patient])
    val extensionChildDefinition = baseResourceDefinition.getChildByName(EXTENSION_ELEMENT_NAME)
    val extensionDefinition = extensionChildDefinition.getChildByName(EXTENSION_ELEMENT_NAME).asInstanceOf[BaseRuntimeElementCompositeDefinition[_]]
    ElementNode(EXTENSION_ELEMENT_NAME, extensionChildDefinition, extensionDefinition)
  }
}
