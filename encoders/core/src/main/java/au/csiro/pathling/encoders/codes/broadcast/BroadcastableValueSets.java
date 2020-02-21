/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2020, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders.codes.broadcast;

import static org.apache.spark.sql.functions.col;

import au.csiro.pathling.encoders.codes.Hierarchies;
import au.csiro.pathling.encoders.codes.base.AbstractValueSets;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.*;

/**
 * An immutable collection of value sets that can be broadcast for use in Spark transformations or
 * user-defined functions.
 */
public class BroadcastableValueSets implements Serializable {

  /**
   * Spark encoder for value set references.
   */
  private static Encoder<Reference> REFERENCE_ENCODER = Encoders.bean(Reference.class);

  /**
   * Spark encoder for hierarchical ancestor values.
   */
  private static Encoder<AncestorValue> ANCESTOR_VALUE_ENCODER = Encoders.bean(AncestorValue.class);

  /**
   * A map from value set reference to code system to a set of values that are contained in that
   * code system.
   */
  private Map<String, Map<String, Set<String>>> values = new HashMap<>();

  /**
   * Private constructor for use in the builder.
   */
  private BroadcastableValueSets(Map<String, Map<String, Set<String>>> values) {

    this.values = values;
  }

  public static class Builder {

    /**
     * Map from value set reference to code system to a set of values that are contained in that
     * code system.
     */
    private Map<String, Map<String, Set<String>>> values = new HashMap<>();

    /**
     * List of reference values to be used when creating a broadcastable value set.
     */
    private List<Reference> references = new ArrayList<>();

    /**
     * List of hierarchical ancestor values to be used when creating the broadcastable value set.
     */
    private List<AncestorValue> ancestorValues = new ArrayList<>();

    /**
     * Adds a code under the given reference name to the value set.
     *
     * @param referenceName the reference name of the value set
     * @param system the code system to add
     * @param code the code value to add
     * @return this builder.
     */
    public Builder addCode(String referenceName, String system, String code) {

      if (this.values == null) {
        throw new IllegalStateException("The builder cannot be used after "
            + "the concept map has been built.");
      }

      Map<String, Set<String>> systemToCodes = this.values.get(referenceName);

      if (systemToCodes == null) {

        systemToCodes = new HashMap<>();

        this.values.put(referenceName, systemToCodes);
      }

      Set<String> codeSet = systemToCodes.get(system);

      if (codeSet == null) {

        codeSet = new HashSet<>();

        systemToCodes.put(system, codeSet);
      }

      codeSet.add(code);

      return this;
    }

    /**
     * Add a "reference" to a value set, that is, create a user-defined reference name for a value
     * set of codes. This method adds a reference using the latest version for the given value set
     * uri.
     *
     * @param referenceName the value set reference name to be used in the user code
     * @param valueSetUri the value set uri that contains codes to reference
     * @return this builder.
     */
    public Builder addReference(String referenceName, String valueSetUri) {

      return addReference(referenceName, valueSetUri, null);
    }

    /**
     * Add a "reference" to a value set, that is, create a user-defined reference name for a value
     * set of codes.
     *
     * @param referenceName the value set reference name to be used in user code
     * @param valueSetUri the value set uri that contains codes to reference
     * @param valueSetVersion the value set version that contains codes to reference
     * @return this builder.
     */
    public Builder addReference(String referenceName, String valueSetUri, String valueSetVersion) {

      this.references.add(new Reference(referenceName,
          valueSetUri,
          valueSetVersion));

      return this;
    }

    /**
     * Add "descendants" of a given code value, that is code values that are transitively subsumed
     * by the given value in the given hierarchy. This uses the latest version of the provided
     * hierarchy.
     *
     * <p>This function creates a collection of ancestors to query, and the descendants
     * are actually retrieved with the broadcastable value set is built.
     *
     * @param referenceName the valueset reference name to be used in user code.
     * @param ancestorSystem the ancestor system of descendants to include
     * @param ancestorValue the ancestor value of descendants to include
     * @param hierarchyUri the hierarchy URI that defines the descendants
     * @return this builder
     */
    public Builder addDescendantsOf(String referenceName,
        String ancestorSystem,
        String ancestorValue,
        String hierarchyUri) {

      return addDescendantsOf(referenceName, ancestorSystem, ancestorValue, hierarchyUri, null);
    }

    /**
     * Add "descendants" of a given code value, that is code values that are transitively subsumed
     * by the given value in the given hierarchy.
     *
     * <p>This function creates a collection of ancestors to query, and the descendants
     * are actually retrieved with the broadcastable value set is built.
     *
     * @param referenceName the valueset reference name to be used in user code.
     * @param ancestorSystem the ancestor system of descendants to include
     * @param ancestorValue the ancestor value of descendants to include
     * @param hierarchyUri the hierarchy URI that defines the descendants
     * @param hierarchyVersion the hierarchy version that defines the descendants
     * @return this builder
     */
    public Builder addDescendantsOf(String referenceName,
        String ancestorSystem,
        String ancestorValue,
        String hierarchyUri,
        String hierarchyVersion) {

      this.ancestorValues.add(new AncestorValue(referenceName,
          hierarchyUri,
          hierarchyVersion,
          ancestorSystem,
          ancestorValue));

      return this;
    }

    /**
     * Adds the version information to any unversioned references.
     *
     * @param valueSets the value sets to use
     */
    private void addReferenceVersions(AbstractValueSets valueSets) {

      // Identify references without versions and load the latest for the value set uri
      Set<String> latestValueSets = this.references.stream()
          .filter(reference -> reference.getValueSetVersion() == null)
          .map(Reference::getValueSetUri)
          .collect(Collectors.toSet());

      final Map<String, String> versions = valueSets.getLatestVersions(latestValueSets, false);

      // Sets the version in references that were not specified
      for (Reference reference : this.references) {

        if (reference.getValueSetVersion() == null) {

          String valueSetVersion = versions.get(reference.getValueSetUri());

          reference.setValueSetVersion(valueSetVersion);
        }
      }
    }

    /**
     * Adds the version information to any unversioned ancestors.
     *
     * @param hierarchies hierarchies to use
     */
    private void addAncestorVersions(Hierarchies hierarchies) {

      // Identify hierarchies without verions and load the latest verions
      Set<String> latestHierarchies = this.ancestorValues.stream()
          .filter(ancestor -> ancestor.version == null)
          .map(AncestorValue::getUri)
          .collect(Collectors.toSet());

      final Map<String, String> versions = hierarchies.getLatestVersions(latestHierarchies);

      for (AncestorValue ancestorValue : this.ancestorValues) {

        if (ancestorValue.getVersion() == null) {

          String version = versions.get(ancestorValue.getUri());

          ancestorValue.setVersion(version);
        }
      }
    }

    /**
     * Returns broadcastable value sets by loading reference data that was added to this builder and
     * the default value sets.
     *
     * @param spark the Spark session used to load reference data
     * @param valueSets a {@link AbstractValueSets} instance defining the value set data to load
     * @return a {@link BroadcastableValueSets} instance.
     */
    public BroadcastableValueSets build(SparkSession spark, AbstractValueSets valueSets) {

      return build(spark, valueSets, Hierarchies.getDefault(spark));
    }

    /**
     * Returns broadcastable value sets by loading reference data that was added to this builder and
     * the given value sets.
     *
     * @param spark the Spark session used to load reference data
     * @param valueSets a {@link AbstractValueSets} instance defining the value set data to load
     * @param hierarchies a {@link Hierarchies} instance defining hierarchical reference data to
     * load
     * @return a {@link BroadcastableValueSets} instance.
     */
    public BroadcastableValueSets build(SparkSession spark,
        AbstractValueSets valueSets,
        Hierarchies hierarchies) {

      // Add pending references and the codes contained in those references' value sets
      if (this.references.size() > 0) {

        // Ensure all references have a version, using latest for any that do not
        addReferenceVersions(valueSets);

        Dataset<Reference> referencesToLoad = spark.createDataset(this.references,
            REFERENCE_ENCODER)
            .as("toload");

        List<Row> codeReferences = valueSets.getValues()
            .as("present")
            .join(
                referencesToLoad,
                col("present.valueSetUri").equalTo(col("toload.valueSetUri"))
                    .and(col("present.valueSetVersion").equalTo(col("toload.valueSetVersion"))))
            .select("referenceName", "system", "value")
            .collectAsList();

        // Add a code for each code held in the reference's value set
        for (Row codeReference : codeReferences) {

          addCode(codeReference.getString(0),
              codeReference.getString(1),
              codeReference.getString(2));
        }
      }

      // Add pending hierarchical descendants to the values
      if (this.ancestorValues.size() > 0) {

        // Ensure all descendants have a version, using the latest for any that do not
        addAncestorVersions(hierarchies);

        Dataset<AncestorValue> ancestorsToLoad = spark.createDataset(this.ancestorValues,
            ANCESTOR_VALUE_ENCODER)
            .as("toload");

        List<Row> descendants = hierarchies.getAncestors()
            .as("present")
            .join(
                ancestorsToLoad,
                col("present.uri").equalTo(col("toload.uri"))
                    .and(col("present.version").equalTo(col("toload.version")))
                    .and(col("present.ancestorSystem").equalTo(col("toload.ancestorSystem")))
                    .and(col("present.ancestorValue").equalTo(col("toload.ancestorValue"))))
            .select("referenceName", "descendantSystem", "descendantValue")
            .collectAsList();

        // Add a code for each descendant
        for (Row descendant : descendants) {

          addCode(descendant.getString(0),
              descendant.getString(1),
              descendant.getString(2));
        }

        // Add a code for the parent as well, since a value is contained in its own value set
        for (AncestorValue ancestorValue : this.ancestorValues) {

          addCode(ancestorValue.getReferenceName(),
              ancestorValue.getAncestorSystem(),
              ancestorValue.getAncestorValue());
        }
      }

      // Nullify the builder values so they cannot be further mutated
      Map<String, Map<String, Set<String>>> values = this.values;

      this.values = null;
      this.references = null;
      this.ancestorValues = null;

      return new BroadcastableValueSets(values);
    }
  }

  /**
   * Returns a {@link BroadcastableValueSets} builder.
   *
   * @return a {@link BroadcastableValueSets} builder.
   */
  public static Builder newBuilder() {

    return new Builder();
  }

  /**
   * Returns true if the value set with the given reference name includes the given code value.
   *
   * @param referenceName the reference name registered for the value set
   * @param system the code system to check if it is contained
   * @param code the code value to check if it is contained
   * @return true if contained, false otherwise.
   * @throws IllegalArgumentException if the referenceName is not known to this object
   */
  public boolean hasCode(String referenceName, String system, String code) {

    Map<String, Set<String>> codeSystemAndValues = this.values.get(referenceName);

    if (codeSystemAndValues == null) {
      throw new IllegalArgumentException("Unknown value set reference name: " + referenceName);
    }

    Set<String> values = codeSystemAndValues.get(system);

    return values == null
        ? false
        : values.contains(code);
  }

  /**
   * Returns the reference names in the value set.
   *
   * @return the set of reference names.
   */
  public Set<String> getReferenceNames() {

    return this.values.keySet();
  }

  /**
   * Returns a map of code systems to the set of code values within those systems used by the given
   * valueset reference.
   *
   * @param referenceName the reference name for which to retrieve values
   * @return a map of the code systems to the code values used by the reference.
   */
  public Map<String, Set<String>> getValues(String referenceName) {

    // Defensively wrap the contained sets in unmodifiable collections. This is done on read since
    // this is an infrequently called operation so we avoid the cost of creating an immutable set
    // for the primary flow where this is not called.
    return this.values.get(referenceName)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            entry -> Collections.unmodifiableSet(entry.getValue())));
  }

  /**
   * Bean-style class to represent a reference to a value set.
   */
  public static class Reference {

    private String referenceName;

    private String valueSetUri;

    private String valueSetVersion;

    /**
     * Nullary constructor so Spark can encode this class as a bean.
     */
    public Reference() {
    }

    /**
     * Constructs a {@link Reference} value.
     *
     * @param referenceName the reference name to be used in user code
     * @param valueSetUri the value set uri defining this relationship
     * @param valueSetVersion the value set version defining this relationship
     */
    public Reference(String referenceName, String valueSetUri, String valueSetVersion) {
      this.referenceName = referenceName;
      this.valueSetUri = valueSetUri;
      this.valueSetVersion = valueSetVersion;
    }

    /**
     * Returns the value set reference name.
     *
     * @return the value set reference name.
     */
    public String getReferenceName() {
      return referenceName;
    }

    /**
     * Sets the value set reference name.
     *
     * @param referenceName the value set reference name
     */
    public void setReferenceName(String referenceName) {
      this.referenceName = referenceName;
    }

    /**
     * Returns the value set uri that defines this reference relationship.
     *
     * @return the value set uri that defines this reference relationship.
     */
    public String getValueSetUri() {
      return valueSetUri;
    }

    /**
     * Sets the value set uri that defines this reference relationship.
     *
     * @param valueSetUri the value set uri that defines this reference relationship
     */
    public void setValueSetUri(String valueSetUri) {
      this.valueSetUri = valueSetUri;
    }

    /**
     * Returns the value set version that defines this reference relationship.
     *
     * @return the value set version that defines this reference relationship.
     */
    public String getValueSetVersion() {
      return valueSetVersion;
    }

    /**
     * Sets the value set version that defines this reference relationship.
     *
     * @param valueSetVersion the value set version that defines this reference relationship
     */
    public void setValueSetVersion(String valueSetVersion) {
      this.valueSetVersion = valueSetVersion;
    }

    @Override
    public boolean equals(Object obj) {

      if (!(obj instanceof Reference)) {
        return false;
      }

      Reference that = (Reference) obj;

      return Objects.equals(this.referenceName, that.referenceName)
          && Objects.equals(this.valueSetUri, that.valueSetUri)
          && Objects.equals(this.valueSetVersion, that.valueSetVersion);
    }

    @Override
    public int hashCode() {
      return 37
          * Objects.hashCode(this.referenceName)
          * Objects.hashCode(this.valueSetUri)
          * Objects.hashCode(this.valueSetVersion);
    }
  }

  /**
   * Bean-style class to represent ancestor values locally and as Spark datasets.
   */
  public static class AncestorValue {

    private String referenceName;

    private String uri;

    private String version;

    private String ancestorSystem;

    private String ancestorValue;

    /**
     * Nullary constructor so Spark can encode this as a bean.
     */
    public AncestorValue() {
    }

    /**
     * Constructs an ancestor value bean.
     *
     * @param referenceName the reference name to be used in user code.
     * @param uri the URI of the hierarchy defining this relationship
     * @param version the version of the hierarchy defining this relationship
     * @param ancestorSystem the code system of the ancestor
     * @param ancestorValue the code value of the ancestor
     */
    public AncestorValue(String referenceName,
        String uri,
        String version,
        String ancestorSystem,
        String ancestorValue) {

      this.referenceName = referenceName;
      this.uri = uri;
      this.version = version;
      this.ancestorSystem = ancestorSystem;
      this.ancestorValue = ancestorValue;
    }

    /**
     * Returns the value set reference name.
     *
     * @return the value set reference name.
     */
    public String getReferenceName() {
      return referenceName;
    }

    /**
     * Sets the value set reference name.
     *
     * @param referenceName the value set reference name
     */
    public void setReferenceName(String referenceName) {
      this.referenceName = referenceName;
    }

    /**
     * Returns the URI of the hierarchy that owns this ancestor value.
     *
     * @return the URI of the hierarchy.
     */
    public String getUri() {
      return uri;
    }

    /**
     * Sets the URI of the concept map that owns this ancestor value.
     *
     * @param uri the URI of the hierarchy
     */
    public void setUri(String uri) {
      this.uri = uri;
    }

    /**
     * Returns the version of the hierarchy that owns this ancestor value.
     *
     * @return the version of the hierarchy.
     */
    public String getVersion() {
      return version;
    }

    /**
     * Sets the version of the hierarchy that owns this ancestor value.
     *
     * @param version the version of the hierarchy
     */
    public void setVersion(String version) {
      this.version = version;
    }

    /**
     * Returns the system of the ancestor code.
     *
     * @return the system of the ancestor code.
     */
    public String getAncestorSystem() {
      return ancestorSystem;
    }

    /**
     * Sets the system of the ancestor code.
     *
     * @param ancestorSystem the system of the ancestor code
     */
    public void setAncestorSystem(String ancestorSystem) {
      this.ancestorSystem = ancestorSystem;
    }

    /**
     * Returns the value of the ancestor code.
     *
     * @return the value of the ancestor code.
     */
    public String getAncestorValue() {
      return ancestorValue;
    }

    /**
     * Sets the value of the ancestor code.
     *
     * @param ancestorValue the value of the ancestor code
     */
    public void setAncestorValue(String ancestorValue) {
      this.ancestorValue = ancestorValue;
    }

    @Override
    public boolean equals(Object obj) {

      if (!(obj instanceof AncestorValue)) {
        return false;
      }

      AncestorValue that = (AncestorValue) obj;

      return Objects.equals(this.referenceName, that.referenceName)
          && Objects.equals(this.uri, that.uri)
          && Objects.equals(this.version, that.version)
          && Objects.equals(this.ancestorSystem, that.ancestorSystem)
          && Objects.equals(this.ancestorValue, that.ancestorValue);
    }

    @Override
    public int hashCode() {
      return 37
          * Objects.hashCode(this.referenceName)
          * Objects.hashCode(this.uri)
          * Objects.hashCode(this.version)
          * Objects.hashCode(this.ancestorSystem)
          * Objects.hashCode(this.ancestorValue);
    }
  }

}
