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
import au.csiro.clinsight.bunsen.codes.UrlAndVersion;
import au.csiro.clinsight.bunsen.codes.Value;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
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
 * This is a partial implementation of logic to manage FHIR ValueSets. It is designed to encapsulate
 * as much functionality as possible while remaining independent of specific FHIR versions. Users
 * should generally not use this class directly, but rather consume the subclass that corresponds to
 * the FHIR version they are using.
 *
 * @param <T> the type of the FHIR ValueSet objects being used
 * @param <C> the type of the subclass of this class being used.
 */
public abstract class AbstractValueSets<T extends IBaseResource, C extends AbstractValueSets<T, C>> {

  /**
   * An encoder for serializing values.
   */
  protected static final Encoder<Value> VALUE_ENCODER = Encoders.bean(Value.class);

  protected static final Encoder<UrlAndVersion> URL_AND_VERSION_ENCODER =
      Encoders.bean(UrlAndVersion.class);

  protected static final Pattern TABLE_NAME_PATTERN =
      Pattern.compile("[A-Za-z][A-Za-z0-9_]*\\.?[A-Za-z0-9_]*");

  /**
   * Returns the encoder for UrlAndVersion tuples.
   *
   * @return the encoder for UrlAndVersion tuples.
   */
  public static Encoder<UrlAndVersion> getUrlAndVersionEncoder() {
    return URL_AND_VERSION_ENCODER;
  }

  /**
   * Returns the encoder for values.
   *
   * @return the encoder for values.
   */
  public static Encoder<Value> getValueEncoder() {
    return AbstractValueSets.VALUE_ENCODER;
  }

  /**
   * Default database name where the value sets information is stored.
   */
  public static final String VALUE_SETS_DATABASE = "ontologies";

  /**
   * Default table name where the expanded values information is stored.
   */
  public static final String VALUES_TABLE = "values";

  /**
   * Default table name where value sets metadata is stored.
   */
  public static final String VALUE_SETS_TABLE = "valuesets";


  protected final SparkSession spark;

  protected final FhirVersionEnum fhirVersion;

  /**
   * URI and Version metadata used to preserve uniqueness among value sets.
   */
  protected final Dataset<UrlAndVersion> members;

  protected final Dataset<Value> values;

  protected final Dataset<T> valueSets;

  protected final Encoder<T> valueSetEncoder;

  protected AbstractValueSets(SparkSession spark,
      FhirVersionEnum fhirVersion,
      Dataset<UrlAndVersion> members,
      Dataset<T> valueSets,
      Dataset<Value> values,
      Encoder<T> valueSetEncoder) {

    this.spark = spark;
    this.fhirVersion = fhirVersion;
    this.members = members;
    this.valueSets = valueSets;
    this.values = values;
    this.valueSetEncoder = valueSetEncoder;
  }

  /**
   * Returns the latest version of all value sets.
   *
   * @param includeExperimental whether to include value sets marked as experimental
   * @return a map of value set URIs to the latest version for them.
   */
  public Map<String, String> getLatestVersions(boolean includeExperimental) {

    return getLatestVersions(null, includeExperimental);
  }

  /**
   * Returns the latest versions of a given set of value sets.
   *
   * @param uris a set of URIs for which to retrieve the latest versions, or null to load them all
   * @param includeExperimental whether to include value sets marked as experimental
   * @return a map of value set URIs to the latest versions for them.
   */
  public Map<String, String> getLatestVersions(final Set<String> uris,
      boolean includeExperimental) {

    // Reduce by the concept map URI to return only the latest version
    // per concept map. Spark's provided max aggregation function
    // only works on numeric types, so we jump into RDDs and perform
    // the reduce by hand.
    JavaRDD<UrlAndVersion> members = this.valueSets.select("url", "version", "experimental")
        .toJavaRDD()
        .filter(row -> (uris == null || uris.contains(row.getString(0)))
            && (includeExperimental || row.isNullAt(2) || !row.getBoolean(2)))
        .mapToPair(row -> new Tuple2<>(row.getString(0), row.getString(1)))
        .reduceByKey((leftVersion, rightVersion) ->
            leftVersion.compareTo(rightVersion) > 0 ? leftVersion : rightVersion)
        .map(tuple -> new UrlAndVersion(tuple._1, tuple._2));

    return spark.createDataset(members.rdd(), URL_AND_VERSION_ENCODER)
        .collectAsList()
        .stream()
        .collect(Collectors.toMap(UrlAndVersion::getUrl,
            UrlAndVersion::getVersion));
  }

  /**
   * Returns a dataset with the latest values for each valueset of the given uris.
   *
   * @param uris URIs for the value sets
   * @param includeExperimental whether to include value sets marked as experimental
   * @return a dataset of the latest mappings for them.
   */
  public Dataset<Value> getLatestValues(Set<String> uris, boolean includeExperimental) {

    // Since mappings are partitioned by URL and version, in most cases it is more efficient to load
    // separately for each partition and union the results.
    Map<String, String> latestVersions = getLatestVersions(uris, includeExperimental);

    return getValues(latestVersions);
  }

  /**
   * Returns a dataset of all values in this collection. This is generally used for inspection and
   * debugging of values.
   *
   * @return a dataset of all values.
   */
  public Dataset<Value> getValues() {
    return this.values;
  }

  /**
   * Returns the values for the given URI and version.
   *
   * @param uri the uri of the value set for which we get values
   * @param version the version of the value set for which we get values
   * @return a dataset of values for the given URI and version.
   */
  public Dataset<Value> getValues(String uri, String version) {

    return this.values.where(col("valueseturi").equalTo(lit(uri))
        .and(col("valuesetversion").equalTo(lit(version))));
  }

  /**
   * Returns a dataset with the values for each element in the map of uri to version.
   *
   * @param uriToVersion a map of value set URI to the version to load
   * @return a dataset of values for the given URIs and versions.
   */
  public Dataset<Value> getValues(Map<String, String> uriToVersion) {

    JavaSparkContext context = new JavaSparkContext(this.spark.sparkContext());

    Broadcast<Map<String, String>> broadcastUrisToVersion = context.broadcast(uriToVersion);

    return this.values.filter((FilterFunction<Value>) value -> {

      String latestVersion = broadcastUrisToVersion.getValue().get(value.getValueSetUri());

      return latestVersion != null && latestVersion.equals(value.getValueSetVersion());
    });
  }

  /**
   * Returns a dataset of value sets to inspect metadata. Since the value sets themselves can be
   * quite large, the values in this dataset do not contain them. Instead, users should use the
   * {@link #getValues()} method to query values in depth.
   *
   * @return a dataset of value sets that do not contain concept values.
   */
  public Dataset<T> getValueSets() {
    return this.valueSets;
  }

  /**
   * Returns the value set with the given uri and version, or null if there is no such value set.
   *
   * @param uri the uri of the value set to return
   * @param version the version of the value set to return
   * @return the specified value set.
   */
  public T getValueSet(String uri, String version) {

    // Load the value sets, which may contain zero items if the value set does not exist

    // Typecast necessary to placate the Java compiler calling this Scala function
    T[] valueSets = (T[]) this.valueSets.filter(
        col("url").equalTo(lit(uri))
            .and(col("version").equalTo(lit(version))))
        .head(1);

    if (valueSets.length == 0) {

      return null;

    } else {

      T valueSet = valueSets[0];

      Dataset<Value> filteredValues = getValues(uri, version);

      addToValueSet(valueSet, filteredValues);

      return valueSet;
    }
  }

  /**
   * Adds the given values to the given value set instance.
   */
  protected abstract void addToValueSet(T valueSet, Dataset<Value> values);

  /**
   * Returns a dataset of distinct URL and version tuples.
   */
  protected Dataset<UrlAndVersion> getUrlAndVersions(Dataset<T> valueSets) {

    return valueSets.select("url", "version")
        .distinct()
        .as(URL_AND_VERSION_ENCODER);
  }

  /**
   * Returns true if the UrlAndVersions if the membersToCheck has any duplicates with the members of
   * this value sets instance.
   */
  protected boolean hasDuplicateUrlAndVersions(Dataset<UrlAndVersion> membersToCheck) {

    return this.members.intersect(membersToCheck).count() > 0;
  }

  /**
   * Returns a new ValueSets instance that includes the given value sets.
   *
   * @param valueSets the value sets to add to the returned collection.
   * @return a new ValueSets instance with the added value sets.
   */
  public abstract C withValueSets(Dataset<T> valueSets);

  /**
   * Returns a new ValueSets instance that includes the given value sets.
   *
   * @param valueSets the value sets to add to the returned collection.
   * @return a new ValueSets instance with the added value sets.
   */
  public C withValueSets(T... valueSets) {

    return withValueSets(Arrays.asList(valueSets));
  }

  /**
   * Returns a new ValueSets instance that includes the given value sets.
   *
   * @param valueSets the value sets to add to the returned collection.
   * @return a new ValueSets instance with the added value sets.
   */
  public C withValueSets(List<T> valueSets) {

    return withValueSets(this.spark.createDataset(valueSets, valueSetEncoder));
  }

  /**
   * Reads all value sets from a given directory and adds them to our collection. The directory may
   * be anything readable from a Spark path, including local filesystems, HDFS, S3, or others.
   *
   * @param path a path from which value sets will be loaded
   * @return an instance of ValueSets that includes the contents from that directory.
   */
  public C withValueSetsFromDirectory(String path) {

    return withValueSets(valueSetDatasetFromDirectory(path));
  }

  /**
   * Returns all value sets that are disjoint with value sets stored in the default database and
   * adds them to our collection. The directory may be anything readable from a Spark path,
   * including local filesystems, HDFS, S3, or others.
   *
   * @param path a path from which disjoint value sets will be loaded
   * @return an instance of ValueSets that includes content from that directory that is disjoint
   * with content already contained in the default database.
   */
  public C withDisjointValueSetsFromDirectory(String path) {

    return withDisjointValueSetsFromDirectory(path, VALUE_SETS_DATABASE);
  }

  /**
   * Returns all value sets that are disjoint with value sets stored in the given database and adds
   * them to our collection. The directory may be anything readable from a Spark path, including
   * local filesystems, HDFS, S3, or others.
   *
   * @param path a path from which disjoint value sets will be loaded
   * @param database the database to check value sets against
   * @return an instance of ValueSets that includes content from that directory that is disjoint
   * with content already contained in the given database.
   */
  public C withDisjointValueSetsFromDirectory(String path, String database) {

    Dataset<UrlAndVersion> currentMembers = this.spark.table(database + "." + VALUE_SETS_TABLE)
        .select("url", "version")
        .distinct()
        .as(URL_AND_VERSION_ENCODER)
        .alias("current");

    Dataset<T> valueSets = valueSetDatasetFromDirectory(path)
        .alias("new")
        .join(currentMembers, col("new.url").equalTo(col("current.url"))
                .and(col("new.version").equalTo(col("current.version"))),
            "leftanti")
        .as(valueSetEncoder);

    return withValueSets(valueSets);
  }

  private static class ToValueSet<T> implements Function<Tuple2<String, String>, T> {

    private FhirVersionEnum fhirVersion;

    private transient IParser xmlParser;

    private transient IParser jsonParser;

    ToValueSet(FhirVersionEnum fhirVersion) {
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

  /**
   * Returns a dataset of ValueSet from the content stored at the given directory.
   */
  protected Dataset<T> valueSetDatasetFromDirectory(String path) {

    JavaRDD<Tuple2<String, String>> fileNamesAndContents = this.spark.sparkContext()
        .wholeTextFiles(path, 1)
        .toJavaRDD();

    return this.spark.createDataset(fileNamesAndContents
        .map(new ToValueSet(fhirVersion))
        .rdd(), valueSetEncoder);
  }

  /**
   * Writes the the value sets to the default database "ontologies" using default table names:
   * "values", and "valuesets".
   */
  public void writeToDatabase() {

    writeToDatabase(VALUE_SETS_DATABASE);
  }

  /**
   * Writes the value sets to the given database using default table names: "values", "valuesets",
   * and "ancestors".
   *
   * @param database the name of the database to which the value sets are saved
   */
  public void writeToDatabase(String database) {

    writeToTables(database + "." + VALUES_TABLE,
        database + "." + VALUE_SETS_TABLE);
  }

  /**
   * Writes value sets to the given tables.
   *
   * <p>Warning: these updates are likely <em>not</em> atomic due to the lack of transactional
   * semantics in the underlying data store. Concurrent users may see previous items removed before
   * new ones are added, or items appear separately than others. This is intended for use in a
   * user-specific sandbox or staging environment.
   *
   * @param valuesTable name of the table to which the value records are saved
   * @param valueSetTable name of the table to which the value set metadata is saved
   */
  public void writeToTables(String valuesTable, String valueSetTable) {

    boolean hasExistingValueSets;

    try {

      spark.sql("DESCRIBE TABLE " + valueSetTable);

      hasExistingValueSets = true;

    } catch (Exception describeException) {

      // Checked exceptions when calling into Scala upset the Java compiler,
      // hence the need for this workaround and re-throw to propagate unexpected
      // failures.
      if (describeException instanceof NoSuchTableException) {

        hasExistingValueSets = false;

      } else {

        throw new RuntimeException(describeException);
      }
    }

    // If the target tables do not exist, we create them. The values and ancestors tables are
    // created explicitly to meet our partitioning system
    if (!hasExistingValueSets) {

      createValuesTable(spark, valuesTable, null);

      // Create a value set table by writing empty data having the proper schema and properties
      spark.emptyDataset(valueSetEncoder)
          .withColumn("timestamp", lit(null).cast("timestamp"))
          .write()
          .format("parquet")
          .partitionBy("timestamp")
          .saveAsTable(valueSetTable);

    }

    // Check existing value set URIs and Versions for duplicates among the new members
    Dataset<UrlAndVersion> currentMembers = this.spark.table(valueSetTable)
        .select("url", "version")
        .distinct()
        .as(URL_AND_VERSION_ENCODER);

    if (hasDuplicateUrlAndVersions(currentMembers)) {

      throw new IllegalArgumentException("The given value sets contains duplicate url and versions "
          + "against value sets already stored in the table, " + valueSetTable);
    }

    writeValuesToTable(this.values, valuesTable);

    this.valueSets.write()
        .mode(SaveMode.ErrorIfExists)
        .insertInto(valueSetTable);
  }

  /**
   * Creates a table of value records partitioned by valueseturi and valuesetversion.
   *
   * @param spark the spark session
   * @param tableName the name of the values table
   * @param location the location to store the table, or null to create a Hive-managed table
   * @throws IllegalArgumentException if the table name or location are malformed
   */
  private static void createValuesTable(SparkSession spark, String tableName, String location) {

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
    builder.append("(system STRING, "
        + "version STRING, "
        + "value STRING)\n"
        + "PARTITIONED BY (valueseturi STRING, valuesetversion STRING)\n");

    builder.append("STORED AS PARQUET\n");

    if (location != null) {
      builder.append("LOCATION '")
          .append(location)
          .append("'");
    }

    spark.sql(builder.toString());
  }

  /**
   * Writes value records to a table. This class ensures the columns and partitions are mapped
   * properly, and is a workaround similar to the problem described <a
   * href="http://stackoverflow.com/questions/35313077/pyspark-order-of-column-on-write-to-mysql-with-jdbc">here</a>.
   *
   * @param values a dataset of value records
   * @param tableName the table to write them to
   */
  private static void writeValuesToTable(Dataset<Value> values, String tableName) {

    // Note the last two columns here must be the partitioned-by columns in order and in lower case
    // for Spark to properly match them to the partitions
    Dataset<Row> orderColumnDataset = values.select("system",
        "version",
        "value",
        "valueseturi",
        "valuesetversion");

    orderColumnDataset.write()
        .mode(SaveMode.ErrorIfExists)
        .insertInto(tableName);
  }
}
