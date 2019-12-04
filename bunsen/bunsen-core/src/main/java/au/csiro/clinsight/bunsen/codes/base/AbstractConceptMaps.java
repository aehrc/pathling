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

package au.csiro.clinsight.bunsen.codes.base;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import au.csiro.clinsight.bunsen.FhirEncoders;
import au.csiro.clinsight.bunsen.codes.Mapping;
import au.csiro.clinsight.bunsen.codes.UrlAndVersion;
import au.csiro.clinsight.bunsen.codes.broadcast.BroadcastableMappings;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.hl7.fhir.instance.model.api.IBaseResource;
import scala.Tuple2;

/**
 * This is a partial implementation of logic to manage FHIR ConceptMaps. It is designed to
 * encapsulate as much functionality as possible while remaining independent of specific FHIR
 * versions. Users should generally not use this class directly, but rather consume the subclass
 * that corresponds to the FHIR version they are using.
 *
 * @param <T> the type of the FHIR ConceptMap objects being used
 * @param <C> the type of the subclass of this class being used.
 */
public abstract class AbstractConceptMaps<T extends IBaseResource,
    C extends AbstractConceptMaps<T, C>> {

  /**
   * An encoder for serializing mappings.
   */
  protected static final Encoder<Mapping> MAPPING_ENCODER = Encoders.bean(Mapping.class);

  protected static final Encoder<UrlAndVersion> URL_AND_VERSION_ENCODER =
      Encoders.bean(UrlAndVersion.class);

  /**
   * Returns the encoder for UrlAndVersion tuples.
   *
   * @return an encoder for UrlAndVersion tuples.
   */
  public static Encoder<UrlAndVersion> getUrlAndVersionEncoder() {
    return URL_AND_VERSION_ENCODER;
  }

  /**
   * Default database name where mapping information is stored.
   */
  public static final String MAPPING_DATABASE = "ontologies";

  /**
   * Default table name where expanded mapping information is stored.
   */
  public static final String MAPPING_TABLE = "mappings";

  /**
   * Defalt table name where concept maps are stored.
   */
  protected static final String CONCEPT_MAP_TABLE = "conceptmaps";

  protected static final Pattern TABLE_NAME_PATTERN =
      Pattern.compile("[A-Za-z][A-Za-z0-9_]*\\.?[A-Za-z0-9_]*");

  protected final SparkSession spark;

  protected final FhirVersionEnum fhirVersion;

  protected final Dataset<UrlAndVersion> members;

  protected final Dataset<T> conceptMaps;

  protected final Dataset<Mapping> mappings;

  protected final Encoder<T> conceptMapEncoder;

  protected AbstractConceptMaps(SparkSession spark,
      FhirVersionEnum fhirVersion,
      Dataset<UrlAndVersion> members,
      Dataset<T> conceptMaps,
      Dataset<Mapping> mappings,
      Encoder<T> conceptMapEncoder) {

    this.spark = spark;
    this.fhirVersion = fhirVersion;
    this.members = members;
    this.conceptMaps = conceptMaps;
    this.mappings = mappings;
    this.conceptMapEncoder = conceptMapEncoder;
  }

  /**
   * Returns a simple dataset of URL and versions of concept maps.
   */
  protected Dataset<UrlAndVersion> getUrlAndVersions(Dataset<T> conceptMaps) {

    return conceptMaps.select(functions.col("url"), functions.col("version"))
        .distinct()
        .as(URL_AND_VERSION_ENCODER);
  }

  private static class ToConceptMap<T> implements Function<Tuple2<String, String>, T> {

    private FhirVersionEnum fhirVersion;

    private transient IParser xmlParser;

    private transient IParser jsonParser;

    ToConceptMap(FhirVersionEnum fhirVersion) {
      this.fhirVersion = fhirVersion;

      xmlParser = FhirEncoders.contextFor(fhirVersion).newXmlParser();
      jsonParser = FhirEncoders.contextFor(fhirVersion).newJsonParser();
    }

    private void writeObject(java.io.ObjectOutputStream stream) throws IOException {

      stream.defaultWriteObject();
    }

    private void readObject(java.io.ObjectInputStream stream) throws IOException,
        ClassNotFoundException {

      stream.defaultReadObject();

      xmlParser = FhirEncoders.contextFor(fhirVersion).newXmlParser();
      jsonParser = FhirEncoders.contextFor(fhirVersion).newJsonParser();
    }

    @Override
    public T call(Tuple2<String, String> fileContentTuple) throws Exception {

      String filePath = fileContentTuple._1.toLowerCase();

      if (filePath.endsWith(".xml")) {

        return (T) xmlParser.parseResource(fileContentTuple._2());

      } else if (filePath.endsWith(".json")) {

        return (T) jsonParser.parseResource(fileContentTuple._2());

      } else {

        throw new RuntimeException("Unrecognized file extension for resource: " + filePath);
      }
    }
  }

  private Dataset<T> conceptMapsDatasetFromDirectory(String path) {

    JavaRDD<Tuple2<String, String>> fileNamesAndContents = this.spark.sparkContext()
        .wholeTextFiles(path, 1)
        .toJavaRDD();

    return this.spark.createDataset(fileNamesAndContents
        .map(new ToConceptMap(fhirVersion))
        .rdd(), conceptMapEncoder);
  }

  /**
   * Returns all concept maps that are disjoint with concept maps stored in the default database and
   * adds them to our collection. The directory may be anything readable from a Spark path,
   * including local filesystems, HDFS, S3, or others.
   *
   * @param path a path from which disjoint concept maps will be loaded
   * @return an instance of ConceptMaps that includes content from that directory that is disjoint
   * with content already contained in the default database.
   */
  public C withDisjointMapsFromDirectory(String path) {

    return withDisjointMapsFromDirectory(path, MAPPING_DATABASE);
  }

  /**
   * Returns all concept maps that are disjoint with concept maps stored in the default database and
   * adds them to our collection. The directory may be anything readable from a Spark path,
   * including local filesystems, HDFS, S3, or others.
   *
   * @param path a path from which disjoint concept maps will be loaded
   * @param database the database to check concept maps against
   * @return an instance of ConceptMaps that includes content from that directory that is disjoint
   * with content already contained in the default database.
   */
  public C withDisjointMapsFromDirectory(String path, String database) {

    Dataset<UrlAndVersion> currentMembers = this.spark
        .sql("SELECT url, version FROM " + database + "." + CONCEPT_MAP_TABLE)
        .as(URL_AND_VERSION_ENCODER)
        .alias("current");

    Dataset<T> maps = conceptMapsDatasetFromDirectory(path)
        .alias("new")
        .join(currentMembers, col("new.url").equalTo(col("current.url"))
                .and(col("new.version").equalTo(col("current.version"))),
            "leftanti")
        .as(conceptMapEncoder);

    return withConceptMaps(maps);
  }

  /**
   * Reads all concept maps from a given directory and adds them to our collection. The directory
   * may be anything readable from a Spark path, including local filesystems, HDFS, S3, or others.
   *
   * @param path a path from which concept maps will be loaded
   * @return a instance of ConceptMaps that includes the contents from that directory.
   */
  public C withMapsFromDirectory(String path) {

    return withConceptMaps(conceptMapsDatasetFromDirectory(path));
  }

  /**
   * Returns a new ConceptMaps instance that includes the given maps.
   *
   * @param conceptMaps concept maps to add to the returned collection.
   * @return a new ConceptMaps instance with the values added.
   */
  public abstract C withConceptMaps(Dataset<T> conceptMaps);

  /**
   * Returns a new ConceptMaps instance that includes the given map.
   *
   * @param conceptMap concept maps to add
   * @return a new ConceptMaps instance with the values added.
   */
  public C withConceptMaps(T... conceptMap) {

    return withConceptMaps(Arrays.asList(conceptMap));
  }

  /**
   * Returns a new ConceptMaps instance that includes the given maps.
   *
   * @param conceptMaps concept maps to add
   * @return a new ConceptMaps instance with the values added.
   */
  public C withConceptMaps(List<T> conceptMaps) {

    return withConceptMaps(this.spark.createDataset(conceptMaps, conceptMapEncoder));
  }

  protected C withConceptMaps(Dataset<T> newMaps, Dataset<Mapping> newMappings) {

    Dataset<UrlAndVersion> newMembers = getUrlAndVersions(newMaps);

    // Instantiating a new composite ConceptMaps requires a new timestamp
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    Dataset<T> newMapsWithTimestamp = newMaps
        .withColumn("timestamp", lit(timestamp.toString()).cast("timestamp"))
        .as(conceptMapEncoder);

    return newInstance(spark,
        this.members.union(newMembers),
        this.conceptMaps.union(newMapsWithTimestamp),
        this.mappings.union(newMappings));
  }

  /**
   * Returns a new instance of this ConceptMaps type with the given parameters.
   */
  protected abstract C newInstance(SparkSession spark,
      Dataset<UrlAndVersion> members,
      Dataset<T> conceptMaps,
      Dataset<Mapping> mappings);

  /**
   * Returns a new ConceptMaps instance that includes the given map and expanded mappings. This
   * method is convenient when mappings themselves are loaded from some ETL operation that produces
   * them.
   *
   * @param conceptMap concept map to add
   * @param mappings dataset of mappings to add to add
   * @return a new ConceptMaps instance with the values added.
   */
  public C withExpandedMap(T conceptMap, Dataset<Mapping> mappings) {

    Dataset<T> conceptMaps = this.spark.createDataset(Arrays.asList(conceptMap),
        conceptMapEncoder);

    return withConceptMaps(conceptMaps, mappings);
  }

  /**
   * Adds the given mappings to the concept map.
   *
   * @param map the concept map
   * @param mappings the mappings to add
   */
  protected abstract void addToConceptMap(T map, Dataset<Mapping> mappings);

  /**
   * Returns the concept map with the given uri and version, or null if there is no such map.
   *
   * @param uri the uri of the map to return
   * @param version the version of the map to return
   * @return the specified concept map.
   */
  public T getConceptMap(String uri, String version) {

    // Load the concept maps, which may contain zero items
    // if the map does not exist.

    // Typecast necessary to placate the Java compiler calling this Scala function.
    T[] maps = (T[]) this.conceptMaps.filter(
        functions.col("url").equalTo(lit(uri))
            .and(functions.col("version").equalTo(lit(version))))
        .head(1);

    if (maps.length == 0) {

      return null;

    } else {

      T map = maps[0];

      Dataset<Mapping> filteredMappings = getMappings(uri, version);

      addToConceptMap(map, filteredMappings);

      return map;
    }
  }

  /**
   * Returns a dataset of concept maps to inspect metadata. Since the mappings themselves can be
   * quite large, the maps in this dataset do not contain them. Instead, users should use the {@link
   * #getMappings()} method to query mappings in depth.
   *
   * @return a dataset of concept maps that do not contain mappings.
   */
  public Dataset<T> getMaps() {
    return this.conceptMaps;
  }

  /**
   * Returns a dataset of all mappings in this collection. This is generally used for inspection and
   * debugging of mappings.
   *
   * @return a dataset of all mappings.
   */
  public Dataset<Mapping> getMappings() {
    return this.mappings;
  }

  /**
   * Returns the mappings for the given URI and version.
   *
   * @param uri the uri of the concept map for which we get mappings
   * @param version the version of the concept map for which we get mappings
   * @return a dataset of mappings for the given URI and version.
   */
  public Dataset<Mapping> getMappings(String uri, String version) {

    return this.mappings.where(functions.col("conceptmapuri").equalTo(lit(uri))
        .and(functions.col("conceptmapversion").equalTo(lit(version))));
  }

  /**
   * Returns a dataset with the mappings for each uri and version.
   *
   * @param uriToVersion a map of concept map URI to the version to load
   * @return a dataset of mappings for the given URIs and versions.
   */
  public Dataset<Mapping> getMappings(Map<String, String> uriToVersion) {

    JavaSparkContext context = new JavaSparkContext(this.spark.sparkContext());

    Broadcast<Map<String, String>> broadcastMaps = context.broadcast(uriToVersion);

    return this.mappings.filter((FilterFunction<Mapping>) mapping -> {

      String latestVersion = broadcastMaps.getValue().get(mapping.getConceptMapUri());

      return latestVersion != null && latestVersion.equals(mapping.getConceptMapVersion());
    });
  }

  /**
   * Returns a dataset with the latest mappings for each valueset here.
   *
   * @param uris URIs for the value sets
   * @param includeExperimental include valuesets labeled as experimental
   * @return a dataset of the latest mappings for them.
   */
  public Dataset<Mapping> getLatestMappings(Set<String> uris, boolean includeExperimental) {

    // Since mappings are partitioned by URL and version, in most cases
    // it is more efficient to load separately for each partition
    // and union the results.
    Map<String, String> latestMaps = getLatestVersions(uris, includeExperimental);

    return getMappings(latestMaps);
  }

  /**
   * Returns the latest versions of all concept maps.
   *
   * @param includeExperimental flag to include concept maps marked as experimental
   * @return a map of concept map URLs to the latest version for them.
   */
  public Map<String, String> getLatestVersions(boolean includeExperimental) {

    return getLatestVersions(null, includeExperimental);
  }

  /**
   * Returns the latest versions of a given set of concept maps.
   *
   * @param urls a set of URLs to retrieve the latest version for, or null to load them all.
   * @param includeExperimental flag to include concept maps marked as experimental
   * @return a map of concept map URLs to the latest version for them.
   */
  public Map<String, String> getLatestVersions(final Set<String> urls,
      boolean includeExperimental) {

    // Reduce by the concept map URI to return only the latest version
    // per concept map. Spark's provided max aggregation function
    // only works on numeric types, so we jump into RDDs and perform
    // the reduce by hand.
    JavaRDD<UrlAndVersion> changes = this.conceptMaps.select(col("url"),
        col("version"),
        col("experimental"))
        .toJavaRDD()
        .filter(row -> (urls == null || urls.contains(row.getString(0)))
            && (includeExperimental || row.isNullAt(2) || !row.getBoolean(2)))
        .mapToPair(row -> new Tuple2<>(row.getString(0), row.getString(1)))
        .reduceByKey((leftVersion, rightVersion) ->
            leftVersion.compareTo(rightVersion) > 0 ? leftVersion : rightVersion)
        .map(tuple -> new UrlAndVersion(tuple._1, tuple._2));

    return this.spark.createDataset(changes.rdd(), URL_AND_VERSION_ENCODER)
        .collectAsList()
        .stream()
        .collect(Collectors.toMap(UrlAndVersion::getUrl,
            UrlAndVersion::getVersion));
  }

  /**
   * Returns true if the UrlAndVersions of new value sets contains duplicates with the current
   * ValueSets.
   */
  protected boolean hasDuplicateUrlAndVersions(Dataset<UrlAndVersion> membersToCheck) {

    return this.members.intersect(membersToCheck).count() > 0;
  }

  /**
   * Creates a table of mapping records partitioned by conceptmapuri and conceptmapversion.
   *
   * @param spark the spark session
   * @param tableName the name of the mapping table
   * @param location the location to store the table, or null to create a Hive-managed table.
   * @throws IllegalArgumentException if the table name or location are malformed.
   */
  private static void createMappingTable(SparkSession spark,
      String tableName,
      String location) {

    if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
      throw new IllegalArgumentException("Invalid table name: " + tableName);
    }

    // Hive will check for well-formed paths, so we just ensure
    // a user isn't attempting to inject additional SQL into the statement.
    if (location != null && location.contains(";")) {
      throw new IllegalArgumentException("Invalid path for mapping table: "
          + location);
    }

    StringBuilder builder = new StringBuilder();

    if (location != null) {

      builder.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");

    } else {

      builder.append("CREATE TABLE IF NOT EXISTS ");
    }

    builder.append(tableName);

    // Note the partitioned by columns are deliberately lower case here,
    // since Spark does not appear to match columns to
    // Hive partitions if they are not.
    builder.append("(sourceValueSet STRING, "
        + "targetValueSet STRING, "
        + "sourceSystem STRING, "
        + "sourceValue STRING, "
        + "targetSystem STRING, "
        + "targetValue STRING, "
        + "equivalence STRING)\n"
        + "PARTITIONED BY (conceptmapuri STRING, conceptmapversion STRING)\n");

    builder.append("STORED AS PARQUET\n");

    if (location != null) {
      builder.append("LOCATION '")
          .append(location)
          .append("'");
    }

    spark.sql(builder.toString());
  }

  /**
   * Writes the updated concept maps to a database using the default "mappings" and "conceptmaps"
   * table names.
   *
   * @param database the database name to which the concepts are saved.
   */
  public void writeToDatabase(String database) {

    writeToTables(database + "." + MAPPING_TABLE,
        database + "." + CONCEPT_MAP_TABLE);
  }

  /**
   * Writes mapping records to a table. This class ensures the columns and partitions are mapped
   * properly, and is a workaround similar to the problem described <a
   * href="http://stackoverflow.com/questions/35313077/pyspark-order-of-column-on-write-to-mysql-with-jdbc">here</a>.
   *
   * @param mappings a dataset of mapping records
   * @param tableName the table to write them to
   */
  private static void writeMappingsToTable(Dataset<Mapping> mappings,
      String tableName) {

    // Note the last two columns here must be the partitioned-by columns
    // in order and in lower case for Spark to properly match
    // them to the partitions.
    Dataset<Row> orderedColumnDataset =
        mappings.select("sourceValueSet",
            "targetValueSet",
            "sourceSystem",
            "sourceValue",
            "targetSystem",
            "targetValue",
            "equivalence",
            "conceptmapuri",
            "conceptmapversion");

    orderedColumnDataset
        .write()
        .insertInto(tableName);
  }

  /**
   * Writes mappings to the given tables.
   *
   * <p>Warning: these updates are likely <em>not</em> atomic due to the lack of transactional
   * semantics in the underlying data store. Concurrent users may see previous items removed before
   * new ones are added, or items appear separately than others. This is intended for use in a
   * user-specific sandbox or staging environment.
   *
   * @param mappingsTable name of the table containing the mapping records
   * @param conceptMapTable name of the table containing the concept map metadata
   */
  public void writeToTables(String mappingsTable, String conceptMapTable) {

    boolean hasExistingMaps;

    try {

      this.spark.sql("describe table " + conceptMapTable);

      hasExistingMaps = true;

    } catch (Exception describeException) {

      // Checked exceptions when calling into Scala upset the Java compiler,
      // hence the need for this workaround and re-throw to propagate unexpected
      // failures.
      if (describeException instanceof NoSuchTableException) {

        hasExistingMaps = false;

      } else {

        throw new RuntimeException(describeException);
      }
    }

    if (!hasExistingMaps) {

      // No target tables exist, so create and write them. The mappings
      // and ancestors tables are created explicitly to meet our
      // partitioning system.
      createMappingTable(this.spark, mappingsTable, null);

      // Create a concept map table by writing empty data having the proper schema and properties
      this.spark.emptyDataset(conceptMapEncoder)
          .withColumn("timestamp", lit(null).cast("timestamp"))
          .write()
          .format("parquet")
          .partitionBy("timestamp")
          .saveAsTable(conceptMapTable);
    }

    Dataset<UrlAndVersion> currentMembers = this.spark
        .sql("SELECT url, version FROM " + conceptMapTable)
        .distinct()
        .as(URL_AND_VERSION_ENCODER);

    if (hasDuplicateUrlAndVersions(currentMembers)) {

      throw new IllegalArgumentException("The given concept maps contains duplicates url and "
          + "versions against concept maps already stored in the table, " + conceptMapTable);
    }

    writeMappingsToTable(this.mappings, mappingsTable);

    this.conceptMaps.write()
        .mode(SaveMode.ErrorIfExists)
        .insertInto(conceptMapTable);
  }

  /**
   * Broadcast mappings stored in the given conceptMaps instance that match the given
   * conceptMapUris.
   *
   * @param conceptMapUris the URIs to broadcast.
   * @param includeExperimental flag to include experimental map versions in the broadcast.
   * @return a broadcast variable containing a mappings object usable in UDFs.
   */
  public Broadcast<BroadcastableMappings> broadcast(Set<String> conceptMapUris,
      boolean includeExperimental) {

    // Load all maps because we must transitively pull in delegated maps, and since
    // there are relatively few maps we can simply do so in one pass.
    Map<String, String> latest = getLatestVersions(includeExperimental);

    return broadcast(latest);
  }

  /**
   * Broadcast mappings stored in the given conceptMaps instance that match the given
   * conceptMapUris.
   *
   * @param conceptMapUriToVersion map of the concept map URIs to broadcast to their versions.
   * @return a broadcast variable containing a mappings object usable in UDFs.
   */
  public abstract Broadcast<BroadcastableMappings> broadcast(
      Map<String, String> conceptMapUriToVersion);
}

