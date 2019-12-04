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

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import scala.Tuple2;

/**
 * An immutable collection of hierarchical systems. This class is used to import ancestor content
 * from hierarchical systems, explore it, and persist it to a database.
 */
public class Hierarchies {

  /**
   * An encoder for serializing ancestors.
   */
  private static final Encoder<Ancestor> ANCESTOR_ENCODER = Encoders.bean(Ancestor.class);

  private static final Encoder<UrlAndVersion> URI_AND_VERSION_ENCODER =
      Encoders.bean(UrlAndVersion.class);

  private static final Encoder<HierarchicalElement> HIERARCHICAL_ELEMENT_ENCODER =
      Encoders.bean(HierarchicalElement.class);

  /**
   * The number of records to put in a slice of expanded ancestors. This just needs to be small
   * enough to fit in a reasonable amount of memory when converting to a Dataset.
   */
  private static final Long ANCESTORS_SLICE_SIZE = 100000L;

  private static final Pattern TABLE_NAME_PATTERN =
      Pattern.compile("[A-Za-z][A-Za-z0-9_]*\\.?[A-Za-z0-9_]*");

  /**
   * Returns the encoder for UrlAndVersion tuples.
   *
   * @return the encoder for UrlAndVersion tuples.
   */
  public static Encoder<UrlAndVersion> getUriAndVersionEncoder() {
    return URI_AND_VERSION_ENCODER;
  }

  /**
   * Returns the encoder for hierarchical elements.
   *
   * @return the encoder for hierarchical elements.
   */
  public static Encoder<HierarchicalElement> getHierarchicalElementEncoder() {
    return HIERARCHICAL_ELEMENT_ENCODER;
  }

  /**
   * Default database name where the ancestor information is stored.
   */
  public static final String HIERARCHIES_DATABASE = "ontologies";

  /**
   * Default table name where expanded ancestor information is stored.
   */
  public static final String ANCESTORS_TABLE = "ancestors";

  /**
   * A URI prefix for hierarchical systems. A Hierarchy URI is "complete" when an identifier suffix
   * for the hierarchy is appended to this value.
   */
  public static final String HIERARCHY_URI_PREFIX = "urn:com:cerner:bunsen:hierarchy:";

  private final SparkSession spark;

  private final Dataset<UrlAndVersion> members;

  private final Dataset<Ancestor> ancestors;

  private Hierarchies(SparkSession spark,
      Dataset<UrlAndVersion> members,
      Dataset<Ancestor> ancestors) {

    this.spark = spark;
    this.members = members;
    this.ancestors = ancestors;
  }

  /**
   * Returns the collection of ancestors from the default database and table.
   *
   * @param spark the spark session
   * @return hierarchies instance.
   */
  public static Hierarchies getDefault(SparkSession spark) {

    return getFromDatabase(spark, HIERARCHIES_DATABASE);
  }

  /**
   * Returns the collection of ancestors from the table in the given database.
   *
   * @param spark the spark session
   * @param database name of the database containing the ancestors table
   * @return a Hierarchies instance.
   */
  public static Hierarchies getFromDatabase(SparkSession spark, String database) {

    Dataset<Ancestor> ancestors = spark.sql("SELECT * FROM " + database + "." + ANCESTORS_TABLE)
        .as(ANCESTOR_ENCODER);

    Dataset<UrlAndVersion> members = ancestors.filter((FilterFunction<Ancestor>) ancestor ->
        ancestor.getUri().startsWith(HIERARCHY_URI_PREFIX))
        .select(col("uri").alias("url"), col("version"))
        .distinct()
        .as(URI_AND_VERSION_ENCODER);

    return new Hierarchies(spark,
        members,
        ancestors);
  }

  /**
   * Returns an empty Hierarchies instance.
   *
   * @param spark the spark session
   * @return an empty Hierarchies instance.
   */
  public static Hierarchies getEmpty(SparkSession spark) {

    return new Hierarchies(spark,
        spark.emptyDataset(URI_AND_VERSION_ENCODER),
        spark.emptyDataset(ANCESTOR_ENCODER));
  }

  /**
   * Returns a dataset of all ancestors in this collection. This is generally used for inspection
   * and debugging of ancestors.
   *
   * @return a dataset of all ancestors.
   */
  public Dataset<Ancestor> getAncestors() {
    return this.ancestors;
  }

  /**
   * Returns a dataset of UrlAndVersion members of this collection.
   *
   * @return a dataset of UrlAndVersion members in this collection.
   */
  public Dataset<UrlAndVersion> getMembers() {
    return this.members;
  }

  /**
   * Returns the latest version of all hierarchies.
   *
   * @return a map of hierarchy URI to the latest version for that hierarchy.
   */
  public Map<String, String> getLatestVersions() {

    return getLatestVersions(null);
  }

  /**
   * Returns latest versions of the given hierarchies.
   *
   * @param uris a set of URIs for which to retrieve the latest versions, or null to load them all
   * @return a map of value set URIs to the latest versions for them.
   */
  public Map<String, String> getLatestVersions(final Set<String> uris) {

    JavaRDD<UrlAndVersion> members = this.members.toJavaRDD()
        .filter(uriAndVersion -> (uris == null || uris.contains(uriAndVersion.getUrl())))
        .mapToPair(uriAndVersion ->
            new Tuple2<>(uriAndVersion.getUrl(), uriAndVersion.getVersion()))
        .reduceByKey((leftVersion, rightVersion) ->
            leftVersion.compareTo(rightVersion) > 0 ? leftVersion : rightVersion)
        .map(tuple -> new UrlAndVersion(tuple._1, tuple._2));

    return spark.createDataset(members.rdd(), URI_AND_VERSION_ENCODER)
        .collectAsList()
        .stream()
        .collect(Collectors.toMap(UrlAndVersion::getUrl,
            UrlAndVersion::getVersion));
  }

  /**
   * Returns a new hierarchies instance with the transitive ancestors computed from the given
   * dataset of {@link HierarchicalElement}.
   *
   * @param hierarchyUri the URI of the hierarchical system to add
   * @param hierarchyVersion the version of the hierarchical system to add
   * @param elements the elements from which to calculate the ancestors
   * @return an instance of Hierarchies with the ancestors computed from the given elements
   */
  public Hierarchies withHierarchyElements(String hierarchyUri,
      String hierarchyVersion,
      Dataset<HierarchicalElement> elements) {

    Dataset<Ancestor> newAncestors = expandElements(hierarchyUri, hierarchyVersion, elements);

    Dataset<UrlAndVersion> newMembers = newAncestors.select(col("uri").alias("url"), col("version"))
        .distinct()
        .as(URI_AND_VERSION_ENCODER);

    if (hasDuplicateUriAndVersions(newMembers)) {

      throw new IllegalArgumentException(
          "Cannot add elements having duplicate hierarchyUri and hierarchyVersion");
    }

    return new Hierarchies(this.spark,
        this.members.union(newMembers),
        this.ancestors.union(newAncestors));
  }

  /**
   * Returns a new hierarchies instance with the given hierarchies.
   *
   * @param hierarchies the hierarchies to add to this instance
   * @return a new instance of Hierarchies.
   */
  public Hierarchies withHierarchies(Hierarchies hierarchies) {

    Dataset<Ancestor> newAncestors = hierarchies.getAncestors();

    Dataset<UrlAndVersion> newMembers = hierarchies.getMembers();

    if (hasDuplicateUriAndVersions(newMembers)) {

      throw new IllegalArgumentException(
          "Cannot add hierarchies having duplicate uri and version");
    }

    return new Hierarchies(this.spark,
        this.members.union(newMembers),
        this.ancestors.union(newAncestors));
  }

  /**
   * A single system,value tuple and its parents. Additional connection types beyond parents may be
   * added as necessary.
   */
  private static class ConceptNode implements Serializable {

    String system;
    String value;

    /**
     * The set of parents. This purposefully relies on the Java default equality semantics, since we
     * only use it internally and it is an efficient way to check for the direct parent of a
     * record.
     */
    Set<ConceptNode> parents;

    ConceptNode(String system, String value) {

      this.system = system;
      this.value = value;
      this.parents = new HashSet<>();
    }

    /**
     * Returns the node's ancestors.
     */
    Set<ConceptNode> getAncestors() {

      Set<ConceptNode> output = new HashSet<>();

      getAncestors(output);

      // The current node is included so we can check for cycles,
      // but it should not produce an ancestor record, so remove it.
      output.remove(this);

      return output;
    }

    private void getAncestors(Set<ConceptNode> visited) {

      // Some input data can contain cycles, so we must explicitly check for that.
      if (!visited.contains(this)) {

        visited.add(this);

        for (ConceptNode parent : parents) {

          parent.getAncestors(visited);
        }
      }
    }
  }

  /**
   * Calculates the transitive closure of ancestor values given the dataset of hierarchical
   * elements.
   */
  private Dataset<Ancestor> expandElements(String hierarchyUri,
      String hierarchyVersion,
      Dataset<HierarchicalElement> elements) {

    // Map used to find previously created concept nodes so we can use them to build a graph
    final Map<String, Map<String, ConceptNode>> conceptNodes = new HashMap<>();

    // List of all nodes for simpler iteration
    final List<ConceptNode> allNodes = new ArrayList<>();

    // Helper function to get or add a node to our collection of nodes
    BiFunction<String, String, ConceptNode> getOrAddNode = (system, value) -> {

      Map<String, ConceptNode> systemMap = conceptNodes.get(system);

      if (systemMap == null) {

        systemMap = new HashMap<>();

        conceptNodes.put(system, systemMap);
      }

      ConceptNode node = systemMap.get(value);

      if (node == null) {

        node = new ConceptNode(system, value);
        systemMap.put(value, node);
        allNodes.add(node);

      }

      return node;
    };

    // Build our graph of nodes
    for (HierarchicalElement element : elements.collectAsList()) {

      ConceptNode node = getOrAddNode.apply(element.getDescendantSystem(),
          element.getDescendantValue());

      ConceptNode parent = getOrAddNode.apply(element.getAncestorSystem(),
          element.getAncestorValue());

      node.parents.add(parent);
    }

    // The graph is built, now translate it into ancestors
    List<Ancestor> ancestors = allNodes.stream()
        .flatMap(node ->
            node.getAncestors()
                .stream()
                .map(ancestorNode ->
                    new Ancestor(hierarchyUri,
                        hierarchyVersion,
                        node.system,
                        node.value,
                        ancestorNode.system,
                        ancestorNode.value)))
        .collect(Collectors.toList());

    // We convert into a sliced RDD, then to a dataset, so we can specify a slice size and prevent
    // Spark from attempting to copy everything at once for very large expansions.
    int slices = (int) (ancestors.size() / ANCESTORS_SLICE_SIZE);

    if (slices > 1) {

      JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

      JavaRDD<Ancestor> rdd = jsc.parallelize(ancestors, slices);

      return spark.createDataset(rdd.rdd(), ANCESTOR_ENCODER);

    } else {

      return spark.createDataset(ancestors, ANCESTOR_ENCODER);
    }
  }

  private boolean hasDuplicateUriAndVersions(Dataset<UrlAndVersion> membersToCheck) {

    return this.members.intersect(membersToCheck).count() > 0;
  }

  /**
   * Writes the ancestors to the default database "ontologies" using the default table "ancestors".
   */
  public void writeToDatabase() {

    writeToDatabase(HIERARCHIES_DATABASE);
  }

  /**
   * Writes the ancestors to the given database using the default table name "ancestors".
   *
   * @param database the name of the database to which the ancestors are saved
   */
  public void writeToDatabase(String database) {

    writeToTables(database + "." + ANCESTORS_TABLE);
  }

  /**
   * Writes the ancestors to the given table.
   *
   * <p>Warning: these updates are likely <em>not</em> atomic due to the lack of transactional
   * semantics in the underlying data store. Concurrent users may see previous items removed before
   * new ones are added, or items appear separately than others. This is intended for use in a
   * user-specific sandbox or staging environment.
   *
   * @param ancestorsTable the name of the table to which the ancestors are saved
   */
  public void writeToTables(String ancestorsTable) {

    boolean hasExistingAncestors;

    try {

      spark.sql("DESCRIBE TABLE " + ancestorsTable);

      hasExistingAncestors = true;

    } catch (Exception describeException) {

      if (describeException instanceof NoSuchTableException) {

        hasExistingAncestors = false;

      } else {

        throw new RuntimeException(describeException);
      }
    }

    if (!hasExistingAncestors) {

      createAncestorsTable(spark, ancestorsTable, null);
    }

    Dataset<UrlAndVersion> currentMembers = this.spark.table(ancestorsTable)
        .select(col("uri").alias("url"), col("version"))
        .distinct()
        .as(URI_AND_VERSION_ENCODER);

    if (hasDuplicateUriAndVersions(currentMembers)) {

      throw new IllegalArgumentException("The given hierarchies contains duplicate uri and "
          + "versions against ancestors already stored in the table, " + ancestorsTable);
    }

    writeAncestorsToTable(this.ancestors, ancestorsTable);
  }

  /**
   * Creates a table of ancestor records partitioned by uri and version.
   *
   * @param spark the spark session
   * @param tableName the name of the ancestors table
   * @param location the location to store the table, or null to create a Hive-managed table
   * @throws IllegalArgumentException if the table name or location are malformed
   */
  private static void createAncestorsTable(SparkSession spark, String tableName, String location) {

    if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
      throw new IllegalArgumentException("Invalid table name: " + tableName);
    }

    // Hive will check for well-formed paths, so we just ensure a user isn't attempting to inject
    // additional SQL into the statement
    if (location != null && location.contains(";")) {
      throw new IllegalArgumentException("Invalid path for values table: " + location);
    }

    StringBuilder builder = new StringBuilder();

    if (location != null) {

      builder.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");

    } else {

      builder.append("CREATE TABLE IF NOT EXISTS ");
    }

    builder.append(tableName);

    // Note the partitioned by columns are deliberately lower case here since Spark does not appear
    // to match columns to Hive partitions if they are not
    builder.append("(descendantSystem STRING, "
        + "descendantValue STRING, "
        + "ancestorSystem STRING,"
        + "ancestorValue String)\n"
        + "PARTITIONED BY (uri STRING, version STRING)\n");

    builder.append("STORED AS PARQUET\n");

    if (location != null) {
      builder.append("LOCATION '")
          .append(location)
          .append("'");
    }

    spark.sql(builder.toString());
  }

  /**
   * Writes ancestor records to a table. This class ensures the columns and partitions are mapped
   * properly, and is a workaround similar to the problem described <a
   * href="http://stackoverflow.com/questions/35313077/pyspark-order-of-column-on-write-to-mysql-with-jdbc">here</a>.
   *
   * @param ancestors a dataset of ancestor records
   * @param tableName the table to write them to
   */
  private static void writeAncestorsToTable(Dataset<Ancestor> ancestors, String tableName) {

    Dataset<Row> orderedColumnDataset = ancestors.select("descendantSystem",
        "descendantValue",
        "ancestorSystem",
        "ancestorValue",
        "uri",
        "version");

    orderedColumnDataset.write()
        .mode(SaveMode.ErrorIfExists)
        .insertInto(tableName);
  }

  /**
   * A JavaBean to represent an element in a hierarchical system from which transitive ancestors can
   * be computed. This class is mutable for easy use as a Spark {@code Dataset<HierarchicalElement>}
   * instance.
   */
  public static class HierarchicalElement implements Serializable {

    private String ancestorSystem;

    private String ancestorValue;

    private String descendantSystem;

    private String descendantValue;

    /**
     * Nullary constructor so Spark can encode this class as a bean.
     */
    public HierarchicalElement() {
    }

    /**
     * Constructs a {@link HierarchicalElement} instance.
     *
     * @param ancestorSystem the ancestor system
     * @param ancestorValue the ancestor value
     * @param descendantSystem the descendant system
     * @param descendantValue the descendant value
     */
    public HierarchicalElement(String ancestorSystem,
        String ancestorValue,
        String descendantSystem,
        String descendantValue) {
      this.ancestorSystem = ancestorSystem;
      this.ancestorValue = ancestorValue;
      this.descendantSystem = descendantSystem;
      this.descendantValue = descendantValue;
    }

    /**
     * Returns the ancestor system.
     *
     * @return the ancestor system.
     */
    public String getAncestorSystem() {
      return ancestorSystem;
    }

    /**
     * Sets the ancestor system.
     *
     * @param ancestorSystem the ancestor system
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

    /**
     * Returns the descendant system.
     *
     * @return the descendant system.
     */
    public String getDescendantSystem() {
      return descendantSystem;
    }

    /**
     * Sets the descendant system.
     *
     * @param descendantSystem the descendant system
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

    @Override
    public boolean equals(Object obj) {

      if (!(obj instanceof HierarchicalElement)) {
        return false;
      }

      HierarchicalElement that = (HierarchicalElement) obj;

      return Objects.equals(this.ancestorSystem, that.ancestorSystem)
          && Objects.equals(this.ancestorValue, that.ancestorValue)
          && Objects.equals(this.descendantSystem, that.descendantSystem)
          && Objects.equals(this.descendantValue, that.descendantValue);
    }

    @Override
    public int hashCode() {
      return 37
          * Objects.hashCode(this.ancestorSystem)
          * Objects.hashCode(this.ancestorValue)
          * Objects.hashCode(this.descendantSystem)
          * Objects.hashCode(this.descendantValue);
    }
  }
}
