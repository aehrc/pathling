/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * @author John Grimes
 */
public class ComplexExpressionBuilder extends ExpressionBuilder {

  public ComplexExpressionBuilder(FHIRDefinedType complexType) {
    super();
    expression.setFhirType(complexType);
    if (complexType == FHIRDefinedType.CODING) {
      expression.setFhirPathType(FhirPathType.CODING);
    }
  }
  
  public static ComplexExpressionBuilder of(FHIRDefinedType complexType) {
    return new ComplexExpressionBuilder(complexType);
  }

}
