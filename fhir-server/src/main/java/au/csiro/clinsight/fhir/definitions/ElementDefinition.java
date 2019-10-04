/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

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
  private final Set<String> childElements = new HashSet<>();

  /**
   * A list of the types of resources this Reference can refer to.
   */
  private final Set<ResourceType> referenceTypes = EnumSet.noneOf(ResourceType.class);

  /**
   * Path to this element, as defined within the StructureDefinition.
   */
  private String path;

  /**
   * FHIR data type for this element, as defined within the StructureDefinition.
   */
  private FHIRDefinedType fhirType;

  /**
   * Maximum permitted cardinality for this element.
   */
  private String maxCardinality;

  public Set<String> getChildElements() {
    return childElements;
  }

  public Set<ResourceType> getReferenceTypes() {
    return referenceTypes;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public FHIRDefinedType getFhirType() {
    return fhirType;
  }

  public void setFhirType(FHIRDefinedType fhirType) {
    this.fhirType = fhirType;
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
