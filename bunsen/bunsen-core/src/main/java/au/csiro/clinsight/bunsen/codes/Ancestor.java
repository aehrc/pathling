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
 * A JavaBean to represent a ancestor row from one value to another. This class is mutable for easy
 * use as a Spark {@code Dataset<Ancestor>} instance.
 */
public class Ancestor implements Serializable {

  private String uri;

  private String version;

  private String descendantSystem;

  private String descendantValue;

  private String ancestorSystem;

  private String ancestorValue;

  /**
   * Nullary constructor so Spark can encode this class as a bean.
   */
  public Ancestor() {
  }

  /**
   * Constructs a {@link Ancestor} instance.
   *
   * @param uri the hierarchy uri that owns this value
   * @param version the hierarchy version that owns this value
   * @param descendantSystem the code system of the descendant value
   * @param descendantValue the descendant value
   * @param ancestorSystem the code system of the ancestor value
   * @param ancestorValue the ancestor value
   */
  public Ancestor(String uri,
      String version,
      String descendantSystem,
      String descendantValue,
      String ancestorSystem,
      String ancestorValue) {
    this.uri = uri;
    this.version = version;
    this.descendantSystem = descendantSystem;
    this.descendantValue = descendantValue;
    this.ancestorSystem = ancestorSystem;
    this.ancestorValue = ancestorValue;
  }

  /**
   * Returns the hierarchy URI that owns this value.
   *
   * @return the hierarchy URI that owns this value.
   */
  public String getUri() {
    return uri;
  }

  /**
   * Sets the hierarchy URI that owns this value.
   *
   * @param uri the hierarchy URI that owns this value
   */
  public void setUri(String uri) {
    this.uri = uri;
  }

  /**
   * Returns the hierarchy version that owns this value.
   *
   * @return the hierarchy version that owns this value.
   */
  public String getVersion() {
    return version;
  }

  /**
   * Sets the hierarchy version that owns this value.
   *
   * @param version the hierarchy version that owns this value
   */
  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * Returns the code system that owns the descendant value.
   *
   * @return the code system that owns the descendant value.
   */
  public String getDescendantSystem() {
    return descendantSystem;
  }

  /**
   * Sets the code system that owns the descendant value.
   *
   * @param descendantSystem the code system that owns the descendant value
   */
  public void setDescendantSystem(String descendantSystem) {
    this.descendantSystem = descendantSystem;
  }

  /**
   * Returns the descendant value.
   *
   * @return the descendant value.
   */
  public String getDescendantValue() {
    return descendantValue;
  }

  /**
   * Sets the descendant value.
   *
   * @param descendantValue the descendant value
   */
  public void setDescendantValue(String descendantValue) {
    this.descendantValue = descendantValue;
  }

  /**
   * Returns the code system that owns the ancestor value.
   *
   * @return the code system that owns the ancestor value.
   */
  public String getAncestorSystem() {
    return ancestorSystem;
  }

  /**
   * Sets the code system that owns the ancestor value.
   *
   * @param ancestorSystem the code system that owns the ancestor value
   */
  public void setAncestorSystem(String ancestorSystem) {
    this.ancestorSystem = ancestorSystem;
  }

  /**
   * Returns the ancestor value.
   *
   * @return the ancestor value.
   */
  public String getAncestorValue() {
    return ancestorValue;
  }

  /**
   * Sets the ancestor value.
   *
   * @param ancestorValue the ancestor value
   */
  public void setAncestorValue(String ancestorValue) {
    this.ancestorValue = ancestorValue;
  }

  @Override
  public boolean equals(Object obj) {

    if (!(obj instanceof Ancestor)) {
      return false;
    }

    Ancestor that = (Ancestor) obj;

    return Objects.equals(this.uri, that.uri)
        && Objects.equals(this.version, that.version)
        && Objects.equals(this.descendantSystem, that.descendantSystem)
        && Objects.equals(this.descendantValue, that.descendantValue)
        && Objects.equals(this.ancestorSystem, that.ancestorSystem)
        && Objects.equals(this.ancestorValue, that.ancestorValue);
  }

  @Override
  public int hashCode() {
    return 37
        * Objects.hashCode(this.uri)
        * Objects.hashCode(this.version)
        * Objects.hashCode(this.descendantSystem)
        * Objects.hashCode(this.descendantValue)
        * Objects.hashCode(this.ancestorSystem)
        * Objects.hashCode(this.ancestorValue);
  }
}
