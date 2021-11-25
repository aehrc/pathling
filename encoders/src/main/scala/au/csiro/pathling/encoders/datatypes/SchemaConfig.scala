package au.csiro.pathling.encoders.datatypes

import ca.uhn.fhir.context.FhirContext

trait SchemaConfig {

  def fhirContext: FhirContext

  def dataTypeMappings: DataTypeMappings

  def maxNestingLevel: Int
}
