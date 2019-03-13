/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author John Grimes
 */
class SummarisedElement {

  @Nonnull
  private final List<String> childElements = new ArrayList<>();
  @Nonnull
  private final List<String> referenceTypes = new ArrayList<>();
  @Nonnull
  private String path;
  @Nonnull
  private String typeCode;
  @Nullable
  private String maxCardinality;

  SummarisedElement(@Nonnull String path, @Nonnull String typeCode) {
    this.path = path;
    this.typeCode = typeCode;
  }

  @Nonnull
  String getPath() {
    return path;
  }

  void setPath(@Nonnull String path) {
    this.path = path;
  }

  @Nonnull
  String getTypeCode() {
    return typeCode;
  }

  void setTypeCode(@Nonnull String typeCode) {
    this.typeCode = typeCode;
  }

  @Nullable
  String getMaxCardinality() {
    return maxCardinality;
  }

  void setMaxCardinality(@Nullable String maxCardinality) {
    this.maxCardinality = maxCardinality;
  }

  @Nonnull
  List<String> getChildElements() {
    return childElements;
  }

  @Nonnull
  public List<String> getReferenceTypes() {
    return referenceTypes;
  }
}
