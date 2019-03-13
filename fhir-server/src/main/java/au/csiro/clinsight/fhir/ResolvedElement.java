/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * @author John Grimes
 */
public class ResolvedElement {

  @Nonnull
  private final LinkedList<MultiValueTraversal> multiValueTraversals = new LinkedList<>();
  @Nonnull
  private final List<String> referenceTypes = new ArrayList<>();
  @Nonnull
  private final String path;
  private String typeCode;
  private ResolvedElementType type;

  ResolvedElement(@Nonnull String path) {
    this.path = path;
  }

  @Nonnull
  public String getPath() {
    return path;
  }

  public String getTypeCode() {
    return typeCode;
  }

  void setTypeCode(String typeCode) {
    this.typeCode = typeCode;
  }

  @Nonnull
  public LinkedList<MultiValueTraversal> getMultiValueTraversals() {
    return multiValueTraversals;
  }

  @Nonnull
  public List<String> getReferenceTypes() {
    return referenceTypes;
  }

  public ResolvedElementType getType() {
    return type;
  }

  public void setType(ResolvedElementType type) {
    this.type = type;
  }

  public enum ResolvedElementType {
    RESOURCE, COMPLEX, BACKBONE, PRIMITIVE, REFERENCE
  }

}
