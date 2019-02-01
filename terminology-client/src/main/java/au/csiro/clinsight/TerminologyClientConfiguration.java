/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

/**
 * @author John Grimes
 */
public class TerminologyClientConfiguration {

  private String terminologyServerUrl;
  private int expansionCount;

  public TerminologyClientConfiguration() {
    terminologyServerUrl = "http://localhost:8080/fhir";
    expansionCount = 1000;
  }

  public String getTerminologyServerUrl() {
    return terminologyServerUrl;
  }

  public void setTerminologyServerUrl(String terminologyServerUrl) {
    this.terminologyServerUrl = terminologyServerUrl;
  }

  public int getExpansionCount() {
    return expansionCount;
  }

  public void setExpansionCount(int expansionCount) {
    this.expansionCount = expansionCount;
  }

  @Override
  public String toString() {
    return "TerminologyClientConfiguration{" +
        "terminologyServerUrl='" + terminologyServerUrl + '\'' +
        ", expansionCount=" + expansionCount +
        '}';
  }
}
