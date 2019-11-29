/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.encoding;

import java.io.Serializable;

/**
 * Used to represent the result of a $validate-code operation. The hash is used for correlation
 * between the input concepts and results, without needing to serialize the entire concept.
 *
 * @author John Grimes
 */
public class ValidateCodeResult implements Serializable {

  private int hash;
  private boolean result;

  public ValidateCodeResult() {
  }

  public ValidateCodeResult(int hash) {
    this.hash = hash;
  }

  public ValidateCodeResult(int hash, boolean result) {
    this.hash = hash;
    this.result = result;
  }

  public int getHash() {
    return hash;
  }

  public void setHash(int hash) {
    this.hash = hash;
  }

  public boolean isResult() {
    return result;
  }

  public void setResult(boolean result) {
    this.result = result;
  }

}
