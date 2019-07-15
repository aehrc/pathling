/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions;

import java.util.LinkedList;

/**
 * A class that holds information about the result of resolving a simple path selection within a
 * FHIRPath expression, including information about any traversals over elements with multiple
 * cardinalities and also the inferred type of the element.
 *
 * @author John Grimes
 */
public class PathTraversal {

  /**
   * Upstream elements on this path which have a maximum cardinality greater than one.
   */
  private final LinkedList<ElementDefinition> multiValueTraversals = new LinkedList<>();

  /**
   * FHIRPath expression for the subject path.
   */
  private String path;

  /**
   * The ElementDefinition that describes the final element within this path.
   */
  private ElementDefinition elementDefinition;

  /**
   * A categorisation of the type of element, inferred through the process of the traversal.
   */
  private ResolvedElementType type;

  public LinkedList<ElementDefinition> getMultiValueTraversals() {
    return multiValueTraversals;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public ElementDefinition getElementDefinition() {
    return elementDefinition;
  }

  public void setElementDefinition(ElementDefinition elementDefinition) {
    this.elementDefinition = elementDefinition;
  }

  public ResolvedElementType getType() {
    return type;
  }

  public void setType(ResolvedElementType type) {
    this.type = type;
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
