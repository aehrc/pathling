/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.library;

import static java.util.Objects.nonNull;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.PathlingVersion;
import au.csiro.pathling.config.EncodingConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.FhirEncoders.Builder;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.sql.FhirpathUDFRegistrar;
import au.csiro.pathling.sql.udf.TerminologyUdfRegistrar;
import au.csiro.pathling.terminology.DefaultTerminologyServiceFactory;
import au.csiro.pathling.terminology.TerminologyFunctions;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.validation.ValidationUtils;
import au.csiro.pathling.views.ConstantDeclarationTypeAdapterFactory;
import au.csiro.pathling.views.StrictStringTypeAdapterFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.functions;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * A class designed to provide access to selected Pathling functionality from a language library
 * context.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@Slf4j
public class PathlingContext {

  private static final String COL_INPUT_CODINGS = "inputCodings";
  private static final String COL_ARG_CODINGS = "argCodings";

  @Nonnull
  @Getter
  private final SparkSession spark;

  @Nonnull
  private final FhirVersionEnum fhirVersion;

  @Nonnull
  @Getter
  private final FhirEncoders fhirEncoders;

  @Nonnull
  @Getter
  private final TerminologyServiceFactory terminologyServiceFactory;

  @Nonnull
  private final TerminologyFunctions terminologyFunctions;

  @Nonnull
  @Getter
  private final Gson gson;

  private PathlingContext(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.spark = spark;
    this.fhirVersion = fhirEncoders.getFhirVersion();
    this.fhirEncoders = fhirEncoders;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.terminologyFunctions = TerminologyFunctions.build();
    TerminologyUdfRegistrar.registerUdfs(spark, terminologyServiceFactory);
    FhirpathUDFRegistrar.registerUDFs(spark);
    gson = buildGson();
  }

  @Nonnull
  private static Gson buildGson() {
    final GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapterFactory(new ConstantDeclarationTypeAdapterFactory());
    builder.registerTypeAdapterFactory(new StrictStringTypeAdapterFactory());
    return builder.create();
  }

  /**
   * Gets the FhirContext for this instance.
   *
   * @return the FhirContext.
   */
  @Nonnull
  public FhirContext getFhirContext() {
    return fhirEncoders.getContext();
  }

  /**
   * Creates a new {@link PathlingContext} using pre-configured {@link SparkSession},
   * {@link FhirEncoders} and {@link TerminologyServiceFactory} objects.
   *
   * @param spark the Spark session to use
   * @param fhirEncoders the FHIR encoders to use
   * @param terminologyServiceFactory the terminology service factory to use
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext create(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    return new PathlingContext(spark, fhirEncoders, terminologyServiceFactory);
  }

  /**
   * Creates a new {@link PathlingContext} with a default setup for Spark, FHIR encoders, and
   * terminology services.
   *
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext create() {
    final SparkSession spark = SparkSession.builder()
        .appName("Pathling")
        .master("local[*]")
        .getOrCreate();

    return create(spark);
  }

  /**
   * Creates a new {@link PathlingContext} using a pre-configured {@link SparkSession}.
   *
   * @param sparkSession the Spark session to use
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext create(@Nonnull final SparkSession sparkSession) {
    final EncodingConfiguration encodingConfig = EncodingConfiguration.builder().build();
    return create(sparkSession, encodingConfig);
  }

  /**
   * Creates a new {@link PathlingContext} using supplied configuration and a pre-configured
   * {@link SparkSession}.
   *
   * @param sparkSession the Spark session to use
   * @param encodingConfig the encoding configuration to use
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext create(@Nonnull final SparkSession sparkSession,
      @Nonnull final EncodingConfiguration encodingConfig) {
    final TerminologyConfiguration terminologyConfig = TerminologyConfiguration.builder().build();
    return create(sparkSession, encodingConfig, terminologyConfig);
  }

  /**
   * Creates a new {@link PathlingContext} using supplied configuration and a pre-configured
   * {@link SparkSession}.
   *
   * @param sparkSession the Spark session to use
   * @param terminologyConfig the terminology configuration to use
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext create(@Nonnull final SparkSession sparkSession,
      @Nonnull final TerminologyConfiguration terminologyConfig) {
    final EncodingConfiguration encodingConfig = EncodingConfiguration.builder().build();
    return create(sparkSession, encodingConfig, terminologyConfig);
  }

  /**
   * Creates a new {@link PathlingContext} using supplied configuration and a pre-configured
   * {@link SparkSession}.
   *
   * @param sparkSession the Spark session to use
   * @param encodingConfiguration the encoding configuration to use
   * @param terminologyConfiguration the terminology configuration to use
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext create(@Nonnull final SparkSession sparkSession,
      @Nonnull final EncodingConfiguration encodingConfiguration,
      @Nonnull final TerminologyConfiguration terminologyConfiguration) {

    ValidationUtils.ensureValid(terminologyConfiguration, "Invalid terminology configuration");
    ValidationUtils.ensureValid(encodingConfiguration, "Invalid encoding configuration");

    final Builder encoderBuilder = getEncoderBuilder(encodingConfiguration);
    final TerminologyServiceFactory terminologyServiceFactory = getTerminologyServiceFactory(
        terminologyConfiguration);
    return create(sparkSession, encoderBuilder.getOrCreate(), terminologyServiceFactory);
  }

  /**
   * Takes a dataset with string representations of FHIR resources and encodes the resources of the
   * given type as a Spark dataset.
   *
   * @param stringResources the dataset with the string representation of the resources.
   * @param resourceClass the class of the resources to encode.
   * @param inputMimeType the mime type of the encoding for the input strings.
   * @param <T> the Java type of the resource
   * @return the dataset with Spark encoded resources.
   */
  @Nonnull
  public <T extends IBaseResource> Dataset<T> encode(@Nonnull final Dataset<String> stringResources,
      @Nonnull final Class<T> resourceClass, @Nonnull final String inputMimeType) {
    final ExpressionEncoder<T> encoder = fhirEncoders.of(resourceClass);
    return stringResources.mapPartitions(
        new EncodeResourceMapPartitions<>(fhirVersion, inputMimeType, resourceClass),
        encoder);
  }


  /**
   * Takes a dataframe with string representations of FHIR resources and encodes the resources of
   * the given type as a Spark dataframe.
   *
   * @param stringResourcesDF the dataframe with the string representation of the resources.
   * @param resourceName the name of the resources to encode.
   * @param inputMimeType the mime type of the encoding for the input strings.
   * @param maybeColumnName the name of the column in the input dataframe that contains the resource
   * strings. If null the input dataframe must have a single column of type string.
   * @return the dataframe with Spark encoded resources.
   */
  @Nonnull
  public Dataset<Row> encode(@Nonnull final Dataset<Row> stringResourcesDF,
      @Nonnull final String resourceName, @Nonnull final String inputMimeType,
      @Nullable final String maybeColumnName) {

    final Dataset<String> stringResources = (nonNull(maybeColumnName)
                                             ? stringResourcesDF.select(maybeColumnName)
                                             : stringResourcesDF).as(Encoders.STRING());

    final RuntimeResourceDefinition definition = FhirEncoders.contextFor(fhirVersion)
        .getResourceDefinition(resourceName);
    return encode(stringResources, definition.getImplementingClass(), inputMimeType).toDF();
  }

  /**
   * Takes a dataframe of encoded resources of the specified type and decodes them into a dataset of
   * string representations, based on the requested output MIME type.
   *
   * @param resources the dataframe of encoded resources
   * @param resourceName the name of the resources to decode
   * @param outputMimeType the MIME type of the output strings
   * @param <T> the type of the resource
   * @return a dataset of string representations of the resources
   */
  @Nonnull
  public <T extends IBaseResource> Dataset<String> decode(@Nonnull final Dataset<Row> resources,
      @Nonnull final String resourceName, @Nonnull final String outputMimeType) {
    final RuntimeResourceDefinition definition = FhirEncoders.contextFor(fhirVersion)
        .getResourceDefinition(resourceName);

    @SuppressWarnings("unchecked")
    final Class<T> resourceClass = (Class<T>) definition.getImplementingClass();

    final ExpressionEncoder<T> encoder = fhirEncoders.of(resourceClass);
    final Dataset<T> typedResources = resources.as(encoder);
    final MapPartitionsFunction<T, String> mapper = new DecodeResourceMapPartitions<>(fhirVersion,
        outputMimeType);

    return typedResources.mapPartitions(mapper, Encoders.STRING());
  }

  /**
   * Takes a dataframe with string representations of FHIR resources and encodes the resources of
   * the given type as a Spark dataframe.
   *
   * @param stringResourcesDF the dataframe with the string representation of the resources. The
   * dataframe must have a single column of type string.
   * @param resourceName the name of the resources to encode.
   * @param inputMimeType the mime type of the encoding for the input strings.
   * @return the dataframe with Spark encoded resources.
   */
  @Nonnull
  public Dataset<Row> encode(@Nonnull final Dataset<Row> stringResourcesDF,
      @Nonnull final String resourceName, @Nonnull final String inputMimeType) {

    return encode(stringResourcesDF, resourceName, inputMimeType, null);
  }

  /**
   * Takes a dataframe with JSON representations of FHIR resources and encodes the resources of the
   * given type as a Spark dataframe.
   *
   * @param stringResourcesDF the dataframe with the JSON representation of the resources. The
   * dataframe must have a single column of type string.
   * @param resourceName the name of the resources to encode.
   * @return the dataframe with Spark encoded resources.
   */
  @Nonnull
  public Dataset<Row> encode(@Nonnull final Dataset<Row> stringResourcesDF,
      @Nonnull final String resourceName) {
    return encode(stringResourcesDF, resourceName, FhirMimeTypes.FHIR_JSON);
  }


  /**
   * Takes a dataset with string representations of FHIR bundles and encodes the resources of the
   * given type as a Spark dataset.
   *
   * @param stringBundles the dataset with the string representation of the resources.
   * @param resourceClass the class of the resources to encode.
   * @param inputMimeType the mime type of the encoding for the input strings.
   * @param <T> the Java type of the resource
   * @return the dataset with Spark encoded resources.
   */
  @Nonnull
  public <T extends IBaseResource> Dataset<T> encodeBundle(
      @Nonnull final Dataset<String> stringBundles, @Nonnull final Class<T> resourceClass,
      @Nonnull final String inputMimeType) {
    return stringBundles.mapPartitions(
        new EncodeBundleMapPartitions<>(fhirVersion, inputMimeType, resourceClass),
        fhirEncoders.of(resourceClass));
  }

  /**
   * Takes a dataframe with string representations of FHIR bundles and encodes the resources of the
   * given type as a Spark dataframe.
   *
   * @param stringBundlesDF the dataframe with the string representation of the resources
   * @param resourceName the name of the resources to encode
   * @param inputMimeType the MIME type of the input strings
   * @param maybeColumnName the name of the column in the input dataframe that contains the bundle
   * strings. If null, the input dataframe must have a single column of type string.
   * @return a Spark dataframe containing the encoded resources
   */
  @Nonnull
  public Dataset<Row> encodeBundle(@Nonnull final Dataset<Row> stringBundlesDF,
      @Nonnull final String resourceName, @Nonnull final String inputMimeType,
      @Nullable final String maybeColumnName) {

    final Dataset<String> stringResources = (nonNull(maybeColumnName)
                                             ? stringBundlesDF.select(maybeColumnName)
                                             : stringBundlesDF).as(Encoders.STRING());

    final RuntimeResourceDefinition definition = FhirEncoders.contextFor(fhirVersion)
        .getResourceDefinition(resourceName);
    return encodeBundle(stringResources, definition.getImplementingClass(), inputMimeType).toDF();
  }

  /**
   * Takes a dataframe with string representations of FHIR bundles and encodes the resources of the
   * given type as a Spark dataframe.
   *
   * @param stringBundlesDF the dataframe with the string representation of the bundles. The
   * dataframe must have a single column of type string.
   * @param resourceName the name of the resources to encode
   * @param inputMimeType the MIME type of the input strings
   * @return a Spark dataframe containing the encoded resources
   */
  @Nonnull
  public Dataset<Row> encodeBundle(@Nonnull final Dataset<Row> stringBundlesDF,
      @Nonnull final String resourceName, @Nonnull final String inputMimeType) {
    return encodeBundle(stringBundlesDF, resourceName, inputMimeType, null);
  }


  /**
   * Takes a dataframe with JSON representations of FHIR bundles and encodes the resources of the
   * given type as a Spark dataframe.
   *
   * @param stringBundlesDF the dataframe with the JSON representation of the resources. The
   * dataframe must have a single column of type string.
   * @param resourceName the name of the resources to encode
   * @return a Spark dataframe containing the encoded resources
   */
  @Nonnull
  public Dataset<Row> encodeBundle(@Nonnull final Dataset<Row> stringBundlesDF,
      @Nonnull final String resourceName) {
    return encodeBundle(stringBundlesDF, resourceName, FhirMimeTypes.FHIR_JSON);
  }

  /**
   * Tests whether the codings within the specified column are members of the specified value set.
   * Creates a new column containing the result.
   *
   * @param dataset the dataset containing the codings
   * @param coding the column containing the codings to test
   * @param valueSetUri the URI of the value set to test against
   * @param outputColumnName the name of the output column
   * @return a new dataset with a new column containing the result
   * @see <a href="https://www.hl7.org/fhir/valueset-operation-validate-code.html">Operation
   * $validate-code on ValueSet</a>
   */
  @Nonnull
  public Dataset<Row> memberOf(@Nonnull final Dataset<Row> dataset, @Nonnull final Column coding,
      @Nonnull final String valueSetUri, @Nonnull final String outputColumnName) {

    final Column codingArrayCol = when(coding.isNotNull(), array(coding)).otherwise(lit(null));

    return terminologyFunctions.memberOf(codingArrayCol, valueSetUri, dataset, outputColumnName);
  }

  /**
   * Translates the codings within the specified column using a concept map known to the terminology
   * service.
   *
   * @param dataset the dataset containing the codings
   * @param coding the column containing the codings to translate
   * @param conceptMapUri the URI of the concept map to use for translation
   * @param reverse if true, the translation will be reversed
   * @param equivalence the CSV representation of the equivalences to use for translation
   * @param target the target value set to translate to
   * @param outputColumnName the name of the output column
   * @return a new dataset with a new column containing the result
   * @see <a href="https://www.hl7.org/fhir/conceptmap-operation-translate.html">Operation
   * $translate on ConceptMap</a>
   */
  @Nonnull
  public Dataset<Row> translate(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Column coding, @Nonnull final String conceptMapUri,
      final boolean reverse, @Nonnull final String equivalence, @Nullable final String target,
      @Nonnull final String outputColumnName) {

    final Column codingArrayCol = when(coding.isNotNull(), array(coding)).otherwise(lit(null));

    final Dataset<Row> translatedDataset = terminologyFunctions.translate(codingArrayCol,
        conceptMapUri, reverse, equivalence, target, dataset, outputColumnName);

    return translatedDataset.withColumn(outputColumnName, functions.col(outputColumnName));
  }

  /**
   * Tests whether one coding subsumes another coding.
   *
   * @param dataset the dataset containing the codings
   * @param leftCoding the column containing the first coding to test
   * @param rightCoding the column containing the second coding to test
   * @param outputColumnName the name of the output column
   * @return a new dataset with a new column containing the result
   * @see <a href="https://www.hl7.org/fhir/codesystem-operation-subsumes.html">Operation $subsumes
   * on CodeSystem</a>
   */
  @Nonnull
  public Dataset<Row> subsumes(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Column leftCoding, @Nonnull final Column rightCoding,
      @Nonnull final String outputColumnName) {

    final Column fromArray = array(leftCoding);
    final Column toArray = array(rightCoding);
    final Column fromCodings = when(leftCoding.isNotNull(), fromArray).otherwise(null);
    final Column toCodings = when(rightCoding.isNotNull(), toArray).otherwise(null);

    final Dataset<Row> idAndCodingSet = dataset.withColumn(COL_INPUT_CODINGS, fromCodings)
        .withColumn(COL_ARG_CODINGS, toCodings);
    return terminologyFunctions.subsumes(idAndCodingSet, idAndCodingSet.col(COL_INPUT_CODINGS),
        idAndCodingSet.col(COL_ARG_CODINGS), outputColumnName, false);
  }

  /**
   * @return a new {@link DataSourceBuilder} that can be used to read from a variety of different
   * data sources
   */
  @Nonnull
  public DataSourceBuilder read() {
    return new DataSourceBuilder(this);
  }

  /**
   * @return the version of the Pathling library
   */
  @Nonnull
  public String getVersion() {
    return new PathlingVersion().getDescriptiveVersion().orElse("UNKNOWN");
  }

  @Nonnull
  private static Builder getEncoderBuilder(@Nonnull final EncodingConfiguration config) {
    return FhirEncoders.forR4()
        .withMaxNestingLevel(config.getMaxNestingLevel())
        .withExtensionsEnabled(config.isEnableExtensions())
        .withOpenTypes(config.getOpenTypes());
  }

  @Nonnull
  private static TerminologyServiceFactory getTerminologyServiceFactory(
      @Nonnull final TerminologyConfiguration configuration) {
    final FhirVersionEnum fhirVersion = FhirContext.forR4().getVersion().getVersion();
    return new DefaultTerminologyServiceFactory(fhirVersion, configuration);
  }

}
