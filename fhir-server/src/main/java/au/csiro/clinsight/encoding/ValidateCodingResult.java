/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.encoding;

import java.io.Serializable;

/**
 * Used to represent the result of a $validate-code operation performed using a Coding.
 *
 * @author John Grimes
 */
public class ValidateCodingResult implements Serializable {

  private Coding value;
  private boolean result;

  public ValidateCodingResult() {
  }

  public ValidateCodingResult(Coding value) {
    this.value = value;
  }

  public ValidateCodingResult(org.hl7.fhir.r4.model.Coding hapiCoding) {
    this.value = new Coding(hapiCoding);
  }

  public Coding getValue() {
    return value;
  }

  public void setValue(Coding value) {
    this.value = value;
  }

  public boolean isResult() {
    return result;
  }

  public void setResult(boolean result) {
    this.result = result;
  }

}
