/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.bunsen.FhirEncoders;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.io.Serializable;
import org.slf4j.Logger;

/**
 * Uses the FhirEncoders class from Bunsen to create a FhirContext, then creates a TerminologyClient
 * with some configuration. Used for code that runs on Spark workers.
 *
 * @author John Grimes
 */
public class TerminologyClientFactory implements Serializable {

  private FhirVersionEnum fhirVersion;
  private String terminologyServerUrl;
  private int socketTimeout;
  private boolean verboseRequestLogging;

  public TerminologyClientFactory() {
  }

  public TerminologyClientFactory(FhirVersionEnum fhirVersion, String terminologyServerUrl,
      int socketTimeout, boolean verboseRequestLogging) {
    this.fhirVersion = fhirVersion;
    this.terminologyServerUrl = terminologyServerUrl;
    this.socketTimeout = socketTimeout;
    this.verboseRequestLogging = verboseRequestLogging;
  }

  public TerminologyClient build(Logger logger) {
    return TerminologyClient
        .build(FhirEncoders.contextFor(fhirVersion), terminologyServerUrl, socketTimeout,
            verboseRequestLogging, logger);
  }

  public FhirVersionEnum getFhirVersion() {
    return fhirVersion;
  }

  public void setFhirVersion(FhirVersionEnum fhirVersion) {
    this.fhirVersion = fhirVersion;
  }

  public String getTerminologyServerUrl() {
    return terminologyServerUrl;
  }

  public void setTerminologyServerUrl(String terminologyServerUrl) {
    this.terminologyServerUrl = terminologyServerUrl;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public void setSocketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  public boolean isVerboseRequestLogging() {
    return verboseRequestLogging;
  }

  public void setVerboseRequestLogging(boolean verboseRequestLogging) {
    this.verboseRequestLogging = verboseRequestLogging;
  }

  @Override
  public String toString() {
    return "TerminologyClientFactory{" +
        "fhirVersion=" + fhirVersion +
        ", terminologyServerUrl='" + terminologyServerUrl + '\'' +
        ", socketTimeout=" + socketTimeout +
        ", verboseRequestLogging=" + verboseRequestLogging +
        '}';
  }

}
