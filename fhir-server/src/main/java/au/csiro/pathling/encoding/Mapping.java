/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.encoding;

import java.io.Serializable;

/**
 * Used to represent the results of $translate and $closure operations.
 *
 * @author John Grimes
 */
public class Mapping implements Serializable {

  private String sourceSystem;

  private String sourceCode;

  private String targetSystem;

  private String targetCode;

  private String equivalence;

  public String getSourceSystem() {
    return sourceSystem;
  }

  public void setSourceSystem(String sourceSystem) {
    this.sourceSystem = sourceSystem;
  }

  public String getSourceCode() {
    return sourceCode;
  }

  public void setSourceCode(String sourceCode) {
    this.sourceCode = sourceCode;
  }

  public String getTargetSystem() {
    return targetSystem;
  }

  public void setTargetSystem(String targetSystem) {
    this.targetSystem = targetSystem;
  }

  public String getTargetCode() {
    return targetCode;
  }

  public void setTargetCode(String targetCode) {
    this.targetCode = targetCode;
  }

  public String getEquivalence() {
    return equivalence;
  }

  public void setEquivalence(String equivalence) {
    this.equivalence = equivalence;
  }

}
