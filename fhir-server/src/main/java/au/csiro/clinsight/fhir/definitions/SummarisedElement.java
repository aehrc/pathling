/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author John Grimes
 */
@SuppressWarnings({"WeakerAccess", "unused"})
class SummarisedElement {

  private final List<String> childElements = new ArrayList<>();
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

  public List<String> getChildElements() {
    return childElements;
  }

  public List<String> getReferenceTypes() {
    return referenceTypes;
  }

  @Nonnull
  public String getPath() {
    return path;
  }

  public void setPath(@Nonnull String path) {
    this.path = path;
  }

  @Nonnull
  public String getTypeCode() {
    return typeCode;
  }

  public void setTypeCode(@Nonnull String typeCode) {
    this.typeCode = typeCode;
  }

  @Nullable
  public String getMaxCardinality() {
    return maxCardinality;
  }

  public void setMaxCardinality(@Nullable String maxCardinality) {
    this.maxCardinality = maxCardinality;
  }

}
