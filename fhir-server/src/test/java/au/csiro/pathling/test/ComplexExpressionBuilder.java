/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
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

}
