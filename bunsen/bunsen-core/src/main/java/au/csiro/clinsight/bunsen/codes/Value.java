/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © Australian e-Health Research Centre,
 * CSIRO. All rights reserved.
 */

package au.csiro.clinsight.bunsen.codes;

import java.io.Serializable;
import java.util.Objects;

/**
 * A JavaBean to represent a value row of a value set. This class is mutable for easy use as a Spark
 * {@code Dataset<Value>} instance.
 */
public class Value implements Serializable {

  private String valueSetUri;

  private String valueSetVersion;

  private String system;

  private String version;

  private String value;

  /**
   * Nullary constructor so Spark can encode this class as a bean.
   */
  public Value() {
  }

  /**
   * Constructs a {@link Value} instance.
   *
   * @param valueSetUri the value set uri that owns this value
   * @param valueSetVersion the value set version that owns this value
   * @param system the code system that owns this value
   * @param version the version of the code system that owns this value
   * @param value the value
   */
  public Value(String valueSetUri,
      String valueSetVersion,
      String system,
      String version,
      String value) {
    this.valueSetUri = valueSetUri;
    this.valueSetVersion = valueSetVersion;
    this.system = system;
    this.version = version;
    this.value = value;
  }

  /**
   * Returns the URI for this FHIR value set that owns this value.
   *
   * @return the URI for this FHIR value set that owns this value.
   */
  public String getValueSetUri() {
    return valueSetUri;
  }

  /**
   * Sets the URI for this FHIR value set that owns this value.
   *
   * @param valueSetUri the URI for this FHIR value set that owns this value
   */
  public void setValueSetUri(String valueSetUri) {
    this.valueSetUri = valueSetUri;
  }

  /**
   * Returns the version for this FHIR value set that owns this value.
   *
   * @return the version for this FHIR value set that owns this value.
   */
  public String getValueSetVersion() {
    return valueSetVersion;
  }

  /**
   * Sets the version for this FHIR value set that owns this value.
   *
   * @param valueSetVersion the version for this FHIR value set that owns this value
   */
  public void setValueSetVersion(String valueSetVersion) {
    this.valueSetVersion = valueSetVersion;
  }

  /**
   * Returns the code system that owns this value.
   *
   * @return the code system that owns this value.
   */
  public String getSystem() {
    return system;
  }

  /**
   * Sets the code system that owns this value.
   *
   * @param system the code system that owns this value
   */
  public void setSystem(String system) {
    this.system = system;
  }

  /**
   * Returns the version of the code system that owns this value.
   *
   * @return the version of the code system that owns this value.
   */
  public String getVersion() {
    return version;
  }

  /**
   * Sets the version of the code system that owns this value.
   *
   * @param version the version of the code system that owns this value
   */
  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * Returns the value.
   *
   * @return the value.
   */
  public String getValue() {
    return value;
  }

  /**
   * Sets the value.
   *
   * @param value the value
   */
  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object obj) {

    if (!(obj instanceof Value)) {
      return false;
    }

    Value that = (Value) obj;

    return Objects.equals(this.valueSetUri, that.valueSetUri)
        && Objects.equals(this.valueSetVersion, that.valueSetVersion)
        && Objects.equals(this.system, that.system)
        && Objects.equals(this.version, that.version)
        && Objects.equals(this.value, that.value);
  }

  @Override
  public int hashCode() {
    return 37
        * Objects.hashCode(this.valueSetUri)
        * Objects.hashCode(this.valueSetVersion)
        * Objects.hashCode(this.system)
        * Objects.hashCode(this.version)
        * Objects.hashCode(this.value);
  }
}
