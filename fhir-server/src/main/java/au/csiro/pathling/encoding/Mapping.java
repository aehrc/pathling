/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.encoding;

import java.io.Serializable;
import org.hl7.fhir.r4.model.Coding;

/**
 * Used to represent the results of $translate and $closure operations.
 *
 * @author John Grimes
 * @author Pior Szul
 */
public class Mapping implements Serializable {

  private static final long serialVersionUID = 1L;

  private SimpleCoding from;
  private SimpleCoding to;
    
  public Mapping(String sourceSystem, String sourceCode, String targetSystem, String targetCode) {
    this.from = new SimpleCoding(sourceSystem, sourceCode);
    this.to = new SimpleCoding(targetSystem, targetCode);
  }
  
  public Mapping() {
  }

  public Mapping(SimpleCoding from, SimpleCoding to) {
    this.from = from;
    this.to = to;
  }

  public SimpleCoding getFrom() {
    return from;
  }

  public void setFrom(SimpleCoding from) {
    this.from = from;
  }

  public SimpleCoding getTo() {
    return to;
  }

  public void setTo(SimpleCoding to) {
    this.to = to;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((from == null) ? 0 : from.hashCode());
    result = prime * result + ((to == null) ? 0 : to.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Mapping other = (Mapping) obj;
    if (from == null) {
      if (other.from != null)
        return false;
    } else if (!from.equals(other.from))
      return false;
    if (to == null) {
      if (other.to != null)
        return false;
    } else if (!to.equals(other.to))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Mapping [from=" + from + ", to=" + to + "]";
  }
 
  public static Mapping of(Coding from, Coding to) {
    return new Mapping(new SimpleCoding(from), new SimpleCoding(to));
  }
  
}
