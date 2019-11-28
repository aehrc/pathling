/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.encoding;

import java.io.Serializable;
import java.util.List;

/**
 * @author John Grimes
 */
public class CodeableConcept implements Serializable {

  private List<Coding> coding;
  private String text;

  public List<Coding> getCoding() {
    return coding;
  }

  public void setCoding(List<Coding> coding) {
    this.coding = coding;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

}
