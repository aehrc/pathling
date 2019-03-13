/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

/**
 * @author John Grimes
 */
public class ReferenceTraversal {

  private String path;
  private String typeCode;

  public ReferenceTraversal(String path, String typeCode) {
    this.path = path;
    this.typeCode = typeCode;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getTypeCode() {
    return typeCode;
  }

  public void setTypeCode(String typeCode) {
    this.typeCode = typeCode;
  }

}
