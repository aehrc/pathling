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

package au.csiro.pathling.bunsen.codes;

import java.util.Objects;

/**
 * A JavaBean to represent a mapping row from one value to another. This class is mutable for easy
 * use as a Spark {@code Dataset<Mapping>} instance.
 */
public class Mapping {

  /**
   * Equivalent constant definition.
   *
   * <p>
   * See <a href="https://www.hl7.org/fhir/valueset-concept-map-equivalence.html">concept map
   * equivalence</a>.
   * </p>
   */
  public static final String EQUIVALENT = "equivalent";

  /**
   * Subsumes constant definition.
   *
   * <p>
   * See <a href="https://www.hl7.org/fhir/valueset-concept-map-equivalence.html">concept map
   * equivalence</a>.
   * </p>
   */
  public static final String SUBSUMES = "subsumes";

  private String conceptMapUri;

  private String conceptMapVersion;

  private String sourceValueSet;

  private String targetValueSet;

  private String sourceSystem;

  private String sourceValue;

  private String targetSystem;

  private String targetValue;

  private String equivalence = EQUIVALENT;

  /**
   * Nullary constructor so Spark can encode this class as a bean.
   */
  public Mapping() {
  }

  /**
   * Constructs a {@link Mapping} instance.
   *
   * @param conceptMapUri the URI for the FHIR concept map that owns this mapping
   * @param conceptMapVersion the version of the FHIR concept map that owns this mapping
   * @param sourceValueSet the valueset for all source values in this mapping
   * @param targetValueSet the valueset for all target values in this mapping
   * @param sourceSystem the code system for the source code
   * @param sourceValue the code value for the source code
   * @param targetSystem the code system for the target code
   * @param targetValue the code value for the target code
   * @param equivalence the FHIR equivalence type
   */
  public Mapping(String conceptMapUri,
      String conceptMapVersion,
      String sourceValueSet,
      String targetValueSet,
      String sourceSystem,
      String sourceValue,
      String targetSystem,
      String targetValue,
      String equivalence) {
    this.conceptMapUri = conceptMapUri;
    this.conceptMapVersion = conceptMapVersion;
    this.sourceValueSet = sourceValueSet;
    this.targetValueSet = targetValueSet;
    this.sourceSystem = sourceSystem;
    this.sourceValue = sourceValue;
    this.targetSystem = targetSystem;
    this.targetValue = targetValue;
    this.equivalence = equivalence;
  }

  /**
   * Returns the URI for the FHIR concept map that owns this mapping.
   *
   * @return the URI for the FHIR concept map that owns this mapping.
   */
  public String getConceptMapUri() {
    return conceptMapUri;
  }

  /**
   * Sets the URI for the FHIR concept map that owns this mapping.
   *
   * @param conceptMapUri the URI for the FHIR concept map that owns this mapping
   */
  public void setConceptMapUri(String conceptMapUri) {
    this.conceptMapUri = conceptMapUri;
  }

  /**
   * Returns the version of the FHIR concept map that owns this mapping.
   *
   * @return the version of the FHIR concept map that owns this mapping.
   */
  public String getConceptMapVersion() {
    return conceptMapVersion;
  }

  /**
   * Sets the version of the FHIR concept map that owns this mapping.
   *
   * @param conceptMapVersion the version of the FHIR concept map that owns this mapping
   */
  public void setConceptMapVersion(String conceptMapVersion) {
    this.conceptMapVersion = conceptMapVersion;
  }

  /**
   * Returns the valueset for all source values in this mapping.
   *
   * @return the valueset for all source values in this mapping.
   */
  public String getSourceValueSet() {
    return sourceValueSet;
  }

  /**
   * Sets the valueset for all source values in this mapping.
   *
   * @param sourceValueSet the valueset for all source values in this mapping
   */
  public void setSourceValueSet(String sourceValueSet) {
    this.sourceValueSet = sourceValueSet;
  }

  /**
   * Returns the valueset for all target values in this mapping.
   *
   * @return the valueset for all target values in this mapping.
   */
  public String getTargetValueSet() {
    return targetValueSet;
  }

  /**
   * Sets the valueset for all target values in this mapping.
   *
   * @param targetValueSet the valueset for all target values in this mapping
   */
  public void setTargetValueSet(String targetValueSet) {
    this.targetValueSet = targetValueSet;
  }

  /**
   * Returns the code system for the source code.
   *
   * @return the code system for the source code.
   */
  public String getSourceSystem() {
    return sourceSystem;
  }

  /**
   * Sets the code system for the source code.
   *
   * @param sourceSystem the code system for the source code
   */
  public void setSourceSystem(String sourceSystem) {
    this.sourceSystem = sourceSystem;
  }

  /**
   * Returns the code value for the source code.
   *
   * @return the code value for the source code.
   */
  public String getSourceValue() {
    return sourceValue;
  }

  /**
   * Sets the code value for the source code.
   *
   * @param sourceValue the code value for the source code
   */
  public void setSourceValue(String sourceValue) {
    this.sourceValue = sourceValue;
  }

  /**
   * Returns the code system for the target code.
   *
   * @return the code system for the target code.
   */
  public String getTargetSystem() {
    return targetSystem;
  }

  /**
   * Sets the code system for the target code.
   *
   * @param targetSystem the code system for the target code
   */
  public void setTargetSystem(String targetSystem) {
    this.targetSystem = targetSystem;
  }

  /**
   * Returns the code value for the target code.
   *
   * @return the code value for the target code.
   */
  public String getTargetValue() {
    return targetValue;
  }

  /**
   * Sets the code value for the target code.
   *
   * @param targetValue the code value for the target code
   */
  public void setTargetValue(String targetValue) {
    this.targetValue = targetValue;
  }


  /**
   * Returns the equivalence for the mapping. Defaults to "equivalent" if not otherwise set.
   *
   * @return the FHIR equivalence type.
   * @see <a href="https://www.hl7.org/fhir/valueset-concept-map-equivalence.html">FHIR valueset
   * concept map equivalence</a>
   */
  public String getEquivalence() {
    return equivalence;
  }

  /**
   * Sets the equivalence for the mapping.
   *
   * @param equivalence the FHIR equivalence type
   * @see <a href="https://www.hl7.org/fhir/valueset-concept-map-equivalence.html">FHIR valueset
   * concept map equivalence</a>
   */
  public void setEquivalence(String equivalence) {
    this.equivalence = equivalence;
  }

  @Override
  public boolean equals(Object obj) {

    if (!(obj instanceof Mapping)) {
      return false;
    }

    Mapping that = (Mapping) obj;

    return Objects.equals(this.conceptMapUri, that.conceptMapUri)
        && Objects.equals(this.conceptMapVersion, that.conceptMapVersion)
        && Objects.equals(this.sourceValueSet, that.sourceValueSet)
        && Objects.equals(this.targetValueSet, that.targetValueSet)
        && Objects.equals(this.sourceSystem, that.sourceSystem)
        && Objects.equals(this.sourceValue, that.sourceValue)
        && Objects.equals(this.targetSystem, that.targetSystem)
        && Objects.equals(this.targetValue, that.targetValue)
        && Objects.equals(this.equivalence, that.equivalence);
  }

  @Override
  public int hashCode() {
    return 37
        * Objects.hashCode(this.conceptMapUri)
        * Objects.hashCode(this.conceptMapVersion)
        * Objects.hashCode(this.sourceValueSet)
        * Objects.hashCode(this.targetValueSet)
        * Objects.hashCode(this.sourceSystem)
        * Objects.hashCode(this.sourceValue)
        * Objects.hashCode(this.targetSystem)
        * Objects.hashCode(this.targetValue)
        * Objects.hashCode(this.equivalence);
  }
}
