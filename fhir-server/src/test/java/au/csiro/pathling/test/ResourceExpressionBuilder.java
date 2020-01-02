/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
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
