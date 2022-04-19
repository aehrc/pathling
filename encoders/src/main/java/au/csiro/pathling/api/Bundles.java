/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.api;

import static java.util.Objects.nonNull;

import au.csiro.pathling.api.definitions.FhirConversionSupport;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.FhirEncoders.Builder;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import scala.Tuple2;

/**
 * Utilities to work with FHIR bundles.
 */
public class Bundles {

  /**
   * A wrapper class for bundles that supports the use of Bundles from various FHIR versions in
   * Spark RDDs.
   */
  public static class BundleContainer implements Serializable {

    private static final long serialVersionUID = -3586519835921650592L;

    private FhirVersionEnum fhirVersion;

    private transient IBaseBundle bundle;

    BundleContainer(IBaseBundle bundle, FhirVersionEnum fhirVersion) {

      this.bundle = bundle;
      this.fhirVersion = fhirVersion;
    }

    private void writeObject(java.io.ObjectOutputStream stream) throws IOException {

      stream.defaultWriteObject();

      String encodedBundle = FhirEncoders.contextFor(fhirVersion)
          .newJsonParser()
          .encodeResourceToString(bundle);

      // TODO: Originally it was using writeUTF but that has limits
      // on the size of the string being written.
      stream.writeObject(encodedBundle);
    }

    private void readObject(java.io.ObjectInputStream stream) throws IOException,
        ClassNotFoundException {

      stream.defaultReadObject();

      String encodedBundle = (String) stream.readObject();

      bundle = (IBaseBundle) FhirEncoders.contextFor(fhirVersion)
          .newJsonParser()
          .parseResource(encodedBundle);
    }

    /**
     * Returns a list containing the resources of the given name that exist in the bundle.
     *
     * @param name the name of the FHIR resource to return
     * @return the resources of the given name that exist in the bundle.
     */
    public List<IBaseResource> extractResource(String name) {

      return FhirConversionSupport.supportFor(fhirVersion).extractEntryFromBundle(bundle, name);
    }

    /**
     * Returns the contained FHIR bundle.
     *
     * @return the contained bundle.
     */
    public IBaseBundle getBundle() {
      return bundle;
    }
  }

  private final FhirVersionEnum fhirVersion;

  private Bundles(FhirVersionEnum fhirVersion) {
    this.fhirVersion = fhirVersion;
  }

  @Nonnull
  public static Bundles forR4() {
    return new Bundles(FhirVersionEnum.R4);
  }

  /**
   * Returns an RDD of bundles loaded from the given path.
   *
   * @param spark the spark session
   * @param path a path to a directory of FHIR Bundles
   * @param minPartitions a suggested value for the minimal number of partitions
   * @return an RDD of FHIR Bundles
   */
  @Nonnull
  public JavaRDD<BundleContainer> loadFromDirectory(@Nonnull final SparkSession spark,
      @Nonnull final String path,
      final int minPartitions) {

    return spark.sparkContext()
        .wholeTextFiles(path, minPartitions)
        .toJavaRDD()
        .map(new ToBundle(fhirVersion));
  }

  /**
   * Returns an RDD of bundles loaded from the given dataset that has JSON-encoded bundles in the
   * given column.
   *
   * @param jsonBundles a dataset of JSON-encoded bundles
   * @param column the column in which the JSON bundle is stored
   * @return an RDD of FHIR Bundles
   */
  @Nonnull
  public JavaRDD<BundleContainer> fromJson(@Nonnull final Dataset<Row> jsonBundles,
      @Nonnull final String column) {

    return fromJson(jsonBundles.select(column).as(Encoders.STRING()));
  }

  /**
   * Returns an RDD of bundles loaded from the given dataset of JSON-encoded bundles.
   *
   * @param jsonBundles a dataset of JSON-encoded bundles
   * @return an RDD of FHIR Bundles
   */
  @Nonnull
  public JavaRDD<BundleContainer> fromJson(@Nonnull final Dataset<String> jsonBundles) {

    return jsonBundles.toJavaRDD().map(new StringToBundle(false, fhirVersion));
  }

  /**
   * Returns an RDD of bundles loaded from the given dataset that has JSON-encoded resources in the
   * given column. Each resource is wrapped in a separate bundle.
   *
   * @param jsonBundles a dataset of JSON-encoded resources
   * @param column the column in which the JSON bundle is stored
   * @return an RDD of FHIR Bundles
   */
  @Nonnull
  public JavaRDD<BundleContainer> fromResourceJson(@Nonnull final Dataset<Row> jsonBundles,
      @Nonnull final String column) {

    return fromResourceJson(jsonBundles.select(column).as(Encoders.STRING()));
  }

  /**
   * Returns an RDD of bundles loaded from the given dataset of JSON-encoded resources. Each
   * resource is wrapped in a separate bundle.
   *
   * @param jsonBundles a dataset of JSON-encoded resources
   * @return an RDD of FHIR Bundles
   */
  @Nonnull
  public JavaRDD<BundleContainer> fromResourceJson(@Nonnull final Dataset<String> jsonBundles) {

    return jsonBundles.toJavaRDD().map(new ResourceStringToBundle(false, fhirVersion));
  }

  /**
   * Returns an RDD of bundles loaded from the given dataset that has XML-encoded bundles in the
   * given column.
   *
   * @param xmlBundles a dataset of XML-encoded bundles
   * @param column the column in which the XML bundle is stored
   * @return an RDD of FHIR Bundles
   */
  @Nonnull
  public JavaRDD<BundleContainer> fromXml(@Nonnull final Dataset<Row> xmlBundles,
      @Nonnull final String column) {

    return fromXml(xmlBundles.select(column).as(Encoders.STRING()));
  }

  /**
   * Returns an RDD of bundles loaded from the given dataset of XML-encoded bundles.
   *
   * @param xmlBundles a dataset of XML-encoded bundles
   * @return an RDD of FHIR Bundles
   */
  @Nonnull
  public JavaRDD<BundleContainer> fromXml(@Nonnull final Dataset<String> xmlBundles) {

    return xmlBundles.toJavaRDD().map(new StringToBundle(true, fhirVersion));
  }

  /**
   * Extracts the given resource type from the RDD of bundles and returns it as a Dataset of that
   * type.
   *
   * @param spark the spark session
   * @param bundles an RDD of FHIR Bundles
   * @param resourceClass the type of resource to extract.
   * @return a dataset of the given resource
   */
  @Nonnull
  public Dataset<Row> extractEntry(@Nonnull final SparkSession spark,
      @Nonnull final JavaRDD<BundleContainer> bundles,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    RuntimeResourceDefinition definition = FhirEncoders.contextFor(fhirVersion)
        .getResourceDefinition(resourceClass);

    return extractEntry(spark, bundles, definition.getName());
  }

  /**
   * Extracts the given resource type from the RDD of bundles and returns it as a Dataset of that
   * type.
   *
   * @param spark the spark session
   * @param bundles an RDD of FHIR Bundles
   * @param resourceName the URL identifying the FHIR resource type or profile.
   * @return a dataset of the given resource
   */
  @Nonnull
  public Dataset<Row> extractEntry(@Nonnull final SparkSession spark,
      @Nonnull final JavaRDD<BundleContainer> bundles,
      @Nonnull final String resourceName) {
    return extractEntry(spark, bundles, resourceName, null, null, null);
  }

  /**
   * Extracts the given resource type from the RDD of bundles and returns it as a Dataset of that
   * type.
   *
   * @param spark the spark session
   * @param bundles an RDD of FHIR Bundles
   * @param resourceName the URL identifying the FHIR resource type or profile.
   * @param maxNestingLevel the maximum nesting level for recursive data types. Zero (0) indicates
   * that all direct or indirect fields of type T in element of type T should be skipped.
   * @param enableExtensions switches on/off the support for FHIR extensions.
   * @param enabledOpenTypes list of types that are encoded within open types, such as extensions.
   * @return a dataset of the given resource
   */
  @Nonnull
  public Dataset<Row> extractEntry(@Nonnull final SparkSession spark,
      @Nonnull final JavaRDD<BundleContainer> bundles,
      @Nonnull final String resourceName,
      @Nullable final Integer maxNestingLevel, @Nullable final Boolean enableExtensions,
      @Nullable final List<String> enabledOpenTypes) {

    final ToResources bundleToResources = new ToResources(resourceName,
        fhirVersion);

    Builder encoderBuilder = FhirEncoders.forR4();
    if (nonNull(maxNestingLevel)) {
      encoderBuilder = encoderBuilder.withMaxNestingLevel(maxNestingLevel);
    }
    if (nonNull(enableExtensions)) {
      encoderBuilder = encoderBuilder.withExtensionsEnabled(enableExtensions);
    }
    if (nonNull(enabledOpenTypes)) {
      encoderBuilder = encoderBuilder
          .withOpenTypes(enabledOpenTypes.stream().collect(Collectors.toUnmodifiableSet()));
    }
    final ExpressionEncoder<IBaseResource> fhirEncoder = encoderBuilder.getOrCreate()
        .of(resourceName);

    final JavaRDD<IBaseResource> resourceRdd = bundles.flatMap(bundleToResources);
    return spark.createDataset(resourceRdd.rdd(), fhirEncoder).toDF();
  }

  private static class StringToBundle implements Function<String, BundleContainer> {

    private static final long serialVersionUID = 1446227753373965483L;

    private boolean isXml;

    private FhirVersionEnum fhirVersion;

    private transient IParser parser;

    StringToBundle(boolean isXml, FhirVersionEnum fhirVersion) {
      this.isXml = isXml;
      this.fhirVersion = fhirVersion;

      parser = isXml
               ? FhirEncoders.contextFor(fhirVersion).newXmlParser()
               : FhirEncoders.contextFor(fhirVersion).newJsonParser();
    }

    private void writeObject(java.io.ObjectOutputStream stream) throws IOException {

      stream.defaultWriteObject();
    }

    private void readObject(java.io.ObjectInputStream stream) throws IOException,
        ClassNotFoundException {

      stream.defaultReadObject();

      parser = isXml
               ? FhirEncoders.contextFor(fhirVersion).newXmlParser()
               : FhirEncoders.contextFor(fhirVersion).newJsonParser();
    }

    @Override
    public BundleContainer call(String bundleString) {

      IBaseBundle bundle = (IBaseBundle) parser.parseResource(bundleString);

      return new BundleContainer(bundle, fhirVersion);
    }
  }


  private static class ResourceStringToBundle implements Function<String, BundleContainer> {

    private static final long serialVersionUID = -4252301955516679442L;

    private boolean isXml;

    private FhirVersionEnum fhirVersion;

    private transient IParser parser;

    private transient FhirConversionSupport support;


    ResourceStringToBundle(boolean isXml, FhirVersionEnum fhirVersion) {
      this.isXml = isXml;
      this.fhirVersion = fhirVersion;

      this.parser = isXml
                    ? FhirEncoders.contextFor(fhirVersion).newXmlParser()
                    : FhirEncoders.contextFor(fhirVersion).newJsonParser();
      this.support = FhirConversionSupport.supportFor(fhirVersion);
    }

    private void writeObject(java.io.ObjectOutputStream stream) throws IOException {

      stream.defaultWriteObject();
    }

    private void readObject(java.io.ObjectInputStream stream) throws IOException,
        ClassNotFoundException {

      stream.defaultReadObject();

      parser = isXml
               ? FhirEncoders.contextFor(fhirVersion).newXmlParser()
               : FhirEncoders.contextFor(fhirVersion).newJsonParser();

      support = FhirConversionSupport.supportFor(fhirVersion);
    }

    @Override
    public BundleContainer call(String resourceString) {

      final IBaseResource resource = parser.parseResource(resourceString);
      final IBaseBundle bundle = support.wrapInBundle(resource);
      return new BundleContainer(bundle, fhirVersion);
    }
  }

  private static class ToBundle implements Function<Tuple2<String, String>, BundleContainer> {

    private static final long serialVersionUID = -2968044192169273050L;

    private FhirVersionEnum fhirVersion;

    private transient IParser xmlParser;

    private transient IParser jsonParser;

    ToBundle(FhirVersionEnum fhirVersion) {
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
    public BundleContainer call(Tuple2<String, String> fileContentTuple) {

      String filePath = fileContentTuple._1.toLowerCase();

      if (filePath.endsWith(".xml")) {

        return new BundleContainer((IBaseBundle) xmlParser.parseResource(fileContentTuple._2()),
            fhirVersion);

      } else if (filePath.endsWith(".json")) {

        return new BundleContainer((IBaseBundle) jsonParser.parseResource(fileContentTuple._2()),
            fhirVersion);

      } else {

        throw new RuntimeException("Unrecognized file extension for resource: " + filePath);
      }
    }
  }

  private static class ToResources implements FlatMapFunction<BundleContainer, IBaseResource> {

    private static final long serialVersionUID = -4045613071523148731L;

    private transient String resourceName;


    private transient FhirVersionEnum fhirVersion;

    private transient FhirConversionSupport support;

    ToResources(String resourceName,
        FhirVersionEnum fhirVersion) {
      this.resourceName = resourceName;
      this.fhirVersion = fhirVersion;

      this.support = FhirConversionSupport.supportFor(fhirVersion);
    }


    private void writeObject(java.io.ObjectOutputStream stream) throws IOException {

      stream.defaultWriteObject();

      stream.writeUTF(resourceName);
      stream.writeObject(fhirVersion);
    }

    private void readObject(java.io.ObjectInputStream stream) throws IOException,
        ClassNotFoundException {

      this.resourceName = stream.readUTF();
      this.fhirVersion = (FhirVersionEnum) stream.readObject();
      this.support = FhirConversionSupport.supportFor(fhirVersion);
    }

    @Override
    public Iterator<IBaseResource> call(BundleContainer bundle) {
      return support.extractEntryFromBundle(bundle.bundle, resourceName).iterator();
    }
  }
}
