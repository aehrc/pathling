/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
public class ResourceExpressionBuilder extends ExpressionBuilder {

  public ResourceExpressionBuilder(ResourceType resourceType, FHIRDefinedType fhirType) {
    super();
    expression.setResource(true);
    expression.setResourceType(resourceType);
    expression.setFhirType(fhirType);
  }

}
