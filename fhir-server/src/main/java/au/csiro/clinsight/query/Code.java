/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

/**
 * This class is used in the creation of Spark Datasets that contain the results of ValueSet
 * expansions.
 *
 * @author John Grimes
 */
public class Code {

  private String system;

  private String code;

  public String getSystem() {
    return system;
  }

  public void setSystem(String system) {
    this.system = system;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }
}
