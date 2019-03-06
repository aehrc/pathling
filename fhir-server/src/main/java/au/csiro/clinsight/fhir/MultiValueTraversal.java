/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import java.util.List;

/**
 * @author John Grimes
 */
@SuppressWarnings("WeakerAccess")
public class MultiValueTraversal {

  private String path;
  private List<String> children;
  private String typeCode;

  public MultiValueTraversal(String path, List<String> children, String typeCode) {
    this.path = path;
    this.children = children;
    this.typeCode = typeCode;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public List<String> getChildren() {
    return children;
  }

  public void setChildren(List<String> children) {
    this.children = children;
  }

  public String getTypeCode() {
    return typeCode;
  }

  public void setTypeCode(String typeCode) {
    this.typeCode = typeCode;
  }

}
