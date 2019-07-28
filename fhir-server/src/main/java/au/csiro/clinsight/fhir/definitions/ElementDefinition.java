/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
  private String path;

  /**
   * FHIR data type for this element, as defined within the StructureDefinition.
   */
  private String typeCode;

  /**
   * Maximum permitted cardinality for this element.
   */
  private String maxCardinality;

  public List<String> getChildElements() {
    return childElements;
  }

  public List<String> getReferenceTypes() {
    return referenceTypes;
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

  public String getMaxCardinality() {
    return maxCardinality;
  }

  public void setMaxCardinality(String maxCardinality) {
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
