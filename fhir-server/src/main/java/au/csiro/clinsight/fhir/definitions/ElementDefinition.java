/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class is used for storing a summarised version of a StructureDefinition in memory, with only
 * the information that is needed by this server.
 *
 * @author John Grimes
 */
public class ElementDefinition {

  /**
   * A list of the names of the child elements of this element.
   */
  private final List<String> childElements = new ArrayList<>();

  /**
   * A list of the types of resources this Reference can refer to.
   */
  private final List<String> referenceTypes = new ArrayList<>();

  /**
   * Path to this element, as defined within the StructureDefinition.
   */
  @Nonnull
  private String path;

  /**
   * FHIR data type for this element, as defined within the StructureDefinition.
   */
  @Nonnull
  private String typeCode;

  /**
   * Maximum permitted cardinality for this element.
   */
  @Nullable
  private String maxCardinality;

  ElementDefinition(@Nonnull String path, @Nonnull String typeCode) {
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

  @Nonnull
  public String getTypeCode() {
    return typeCode;
  }

  @Nullable
  public String getMaxCardinality() {
    return maxCardinality;
  }

  public void setMaxCardinality(@Nonnull String maxCardinality) {
    this.maxCardinality = maxCardinality;
  }

  /**
   * Two ElementDefinitions are equal if their paths are equal.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ElementDefinition that = (ElementDefinition) o;
    return path.equals(that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }
}
