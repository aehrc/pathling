/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A class that holds summarised information about a FHIR element resolved from a FHIRPath
 * expression, using a set of known definitions.
 *
 * @author John Grimes
 */
public class ResolvedElement {

  @Nonnull
  private final String path;
  private final LinkedList<MultiValueTraversal> multiValueTraversals = new LinkedList<>();
  private final List<String> referenceTypes = new ArrayList<>();
  @Nullable
  private String typeCode;
  @Nullable
  private ResolvedElementType type;

  ResolvedElement(@Nonnull String path) {
    this.path = path;
  }

  @Nonnull
  public String getPath() {
    return path;
  }

  @Nullable
  public String getTypeCode() {
    return typeCode;
  }

  public void setTypeCode(@Nullable String typeCode) {
    this.typeCode = typeCode;
  }

  @Nullable
  public ResolvedElementType getType() {
    return type;
  }

  public void setType(@Nullable ResolvedElementType type) {
    this.type = type;
  }

  public LinkedList<MultiValueTraversal> getMultiValueTraversals() {
    return multiValueTraversals;
  }

  public List<String> getReferenceTypes() {
    return referenceTypes;
  }

  /**
   * A categorisation of FHIR element type. Examples:
   *
   * RESOURCE - Patient
   *
   * COMPLEX - Patient.photo
   *
   * BACKBONE - Patient.contact
   *
   * PRIMITIVE - Patient.active
   *
   * REFERENCE - Patient.generalPractitioner
   */
  public enum ResolvedElementType {
    RESOURCE, COMPLEX, BACKBONE, PRIMITIVE, REFERENCE
  }

}
