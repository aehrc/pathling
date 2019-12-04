/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

/**
 * @author John Grimes
 */
public class ContentItemParameter {

  public String identifier;
  public String version;
  public String categoryTerm;

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getCategoryTerm() {
    return categoryTerm;
  }

  public void setCategoryTerm(String categoryTerm) {
    this.categoryTerm = categoryTerm;
  }

}
