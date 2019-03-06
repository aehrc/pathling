/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import java.util.ArrayList;
import java.util.List;

/**
 * @author John Grimes
 */
public class ResolvedElement {

  private final List<MultiValueTraversal> multiValueTraversals = new ArrayList<>();
  private final String path;
  private String typeCode;
  private ResolvedElementType type;

  ResolvedElement(String path) {
    this.path = path;
  }

  String getPath() {
    return path;
  }

  public String getTypeCode() {
    return typeCode;
  }

  void setTypeCode(String typeCode) {
    this.typeCode = typeCode;
  }

  public List<MultiValueTraversal> getMultiValueTraversals() {
    return multiValueTraversals;
  }

  public ResolvedElementType getType() {
    return type;
  }

  public void setType(ResolvedElementType type) {
    this.type = type;
  }

  public enum ResolvedElementType {
    RESOURCE, COMPLEX, BACKBONE, PRIMITIVE
  }
}
