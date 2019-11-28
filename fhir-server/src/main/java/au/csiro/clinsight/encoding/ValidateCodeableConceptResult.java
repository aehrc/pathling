/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.encoding;

import java.io.Serializable;

/**
 * Used for storing the result of a $validate-code operation performed using a CodeableConcept.
 *
 * @author John Grimes
 */
public class ValidateCodeableConceptResult implements Serializable {

  private CodeableConcept codeableConcept;
  private boolean result;

  public ValidateCodeableConceptResult() {
  }

  public ValidateCodeableConceptResult(CodeableConcept codeableConcept) {
    this.codeableConcept = codeableConcept;
  }

  public CodeableConcept getCodeableConcept() {
    return codeableConcept;
  }

  public void setCodeableConcept(CodeableConcept codeableConcept) {
    this.codeableConcept = codeableConcept;
  }

  public boolean isResult() {
    return result;
  }

  public void setResult(boolean result) {
    this.result = result;
  }

}
