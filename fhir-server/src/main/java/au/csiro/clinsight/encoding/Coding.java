/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.encoding;

import java.io.Serializable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * @author John Grimes
 */
public class Coding implements Serializable {

  private String system;
  private String version;
  private String code;
  private String display;
  private boolean userSelected;

  public Coding() {
  }

  public Coding(String system, String code, String display) {
    this.system = system;
    this.code = code;
    this.display = display;
  }

  public Coding(org.hl7.fhir.r4.model.Coding hapiCoding) {
    this.system = hapiCoding.getSystem();
    this.version = hapiCoding.getVersion();
    this.code = hapiCoding.getCode();
    this.display = hapiCoding.getDisplay();
    this.userSelected = hapiCoding.getUserSelected();
  }

  public String getSystem() {
    return system;
  }

  public void setSystem(String system) {
    this.system = system;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getDisplay() {
    return display;
  }

  public void setDisplay(String display) {
    this.display = display;
  }

  public boolean isUserSelected() {
    return userSelected;
  }

  public void setUserSelected(boolean userSelected) {
    this.userSelected = userSelected;
  }

  public org.hl7.fhir.r4.model.Coding toHapiCoding() {
    org.hl7.fhir.r4.model.Coding hapiCoding = new org.hl7.fhir.r4.model.Coding(system, code,
        display);
    hapiCoding.setVersion(version);
    hapiCoding.setUserSelected(userSelected);
    return hapiCoding;
  }

  /**
   * This is needed because of the fact that there is no way to enforce the ordering of the struct
   * fields emitted by the Spark bean encoder. Order matters when comparing structs on equality.
   *
   * Alternatives to this could include writing native encoders for these types that we need to
   * return from mapping operations, or somehow modifying Bunsen to allow it to provide encoders for
   * composite types.
   */
  public static Column reorderStructFields(Column coding) {
    return functions
        .struct(coding.getField("system").alias("system"),
            coding.getField("version").alias("version"), coding.getField("code").alias("code"),
            coding.getField("display").alias("display"),
            coding.getField("userSelected").alias("userSelected"));
  }

}
