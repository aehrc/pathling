/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes.encoding;

import au.csiro.pathling.fhir.SimpleCoding;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IdAndCodingSets implements Serializable {

  private static final long serialVersionUID = 1L;

  private String id;
  private List<SimpleCoding> leftCodings;
  private List<SimpleCoding> rightCodings;
  
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public List<SimpleCoding> getLeftCodings() {
    return leftCodings;
  }
  public void setLeftCodings(List<SimpleCoding> leftCodings) {
    this.leftCodings = leftCodings;
  }
  public List<SimpleCoding> getRightCodings() {
    return rightCodings;
  }
  public void setRightCodings(List<SimpleCoding> rightCodings) {
    this.rightCodings = rightCodings;
  }
  
  public boolean subsumes() {
     Set<SimpleCoding> ls = new HashSet<SimpleCoding>(rightCodings);
     ls.retainAll(leftCodings);
     return !ls.isEmpty();
  }
}
