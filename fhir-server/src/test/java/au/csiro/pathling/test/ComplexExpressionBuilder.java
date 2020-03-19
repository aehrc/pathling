/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.TestUtilities;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import org.hl7.fhir.instance.model.api.IBaseDatatype;
import org.hl7.fhir.instance.model.api.IBaseResource;
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

  public ComplexExpressionBuilder withDefinitionFromResource(
      Class<? extends IBaseResource> resourceClazz, String elementName) {
    RuntimeResourceDefinition resourceDefinition = TestUtilities.getFhirContext()
        .getResourceDefinition(resourceClazz);
    expression.setDefinition(resourceDefinition.getChildByName(elementName), elementName);
    return this;
  }

  public ComplexExpressionBuilder withDefinitionFromComposite(
      Class<? extends IBaseDatatype> compositeClazz, String elementName) {
    BaseRuntimeElementCompositeDefinition<?> elementDefinition = (BaseRuntimeElementCompositeDefinition<?>) TestUtilities
        .getFhirContext().getElementDefinition(compositeClazz);
    expression.setDefinition(elementDefinition.getChildByName(elementName), elementName);
    return this;
  }

  public static ComplexExpressionBuilder of(FHIRDefinedType complexType) {
    return new ComplexExpressionBuilder(complexType);
  }

}
