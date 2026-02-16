/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.PathlingVersion;
import au.csiro.pathling.config.EncodingConfiguration;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.encoders.FhirEncoderBuilder;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ResourceTypes;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.CrossResourceStrategy;
import au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluator;
import au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.library.FhirPathResult.TypedValue;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.search.SearchColumnBuilder;
import au.csiro.pathling.sql.PathlingUdfConfigurer;
import au.csiro.pathling.sql.udf.TerminologyUdfRegistrar;
import au.csiro.pathling.terminology.DefaultTerminologyServiceFactory;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A class designed to provide access to selected Pathling functionality from a language library
 * context.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@Slf4j
public class PathlingContext {

  /**
   * The MIME type for FHIR JSON resources.
   *
   * @see <a href="https://www.hl7.org/fhir/json.html">FHIR JSON</a>
   */
  public static final String FHIR_JSON = "application/fhir+json";

  /**
   * The MIME type for FHIR XML resources.
   *
   * @see <a href="https://www.hl7.org/fhir/xml.html">FHIR XML</a>
   */
  public static final String FHIR_XML = "application/fhir+xml";

  @Nonnull @Getter private final SparkSession spark;

  @Nonnull private final FhirVersionEnum fhirVersion;

  @Nonnull @Getter private final FhirEncoders fhirEncoders;

  @Nonnull @Getter private final TerminologyServiceFactory terminologyServiceFactory;

  @Nonnull @Getter private final QueryConfiguration queryConfiguration;

  @Nonnull @Getter private final Gson gson;

  /**
   * Creates a new PathlingContext with the specified configuration.
   *
   * @param spark the Spark session to use
   * @param fhirEncoders the FHIR encoders to use
   * @param terminologyServiceFactory the terminology service factory to use
   * @param queryConfiguration the query configuration to use
   */
  private PathlingContext(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final QueryConfiguration queryConfiguration) {
    this.spark = spark;
    this.fhirVersion = fhirEncoders.getFhirVersion();
    this.fhirEncoders = fhirEncoders;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.queryConfiguration = queryConfiguration;
    TerminologyUdfRegistrar.registerUdfs(spark, terminologyServiceFactory);
    PathlingUdfConfigurer.registerUdfs(spark);
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
   * Creates a {@link PathlingContext} with advanced configuration for testing purposes. This method
   * is internal and should not be used by library consumers but it needs to be public to be
   * accessible from other packages in the module.
   *
   * @param spark the Spark session to use
   * @param fhirEncoders the FHIR encoders to use
   * @param terminologyServiceFactory the terminology service factory to use
   * @return a new {@link PathlingContext} instance with default query configuration
   */
  @Nonnull
  public static PathlingContext createInternal(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    return new PathlingContext(
        spark, fhirEncoders, terminologyServiceFactory, QueryConfiguration.builder().build());
  }

  /** Builder for creating {@link PathlingContext} instances with configurable options. */
  public static class Builder {

    @Nullable private SparkSession spark;
    @Nullable private EncodingConfiguration encodingConfiguration;
    @Nullable private TerminologyConfiguration terminologyConfiguration;
    @Nullable private QueryConfiguration queryConfiguration;

    Builder() {}

    Builder(@Nullable final SparkSession spark) {
      this.spark = spark;
    }

    /**
     * Sets the Spark session for the context.
     *
     * @param spark the Spark session to use, or null to use a default Spark session
     * @return this builder
     */
    @Nonnull
    public Builder spark(@Nullable final SparkSession spark) {
      this.spark = spark;
      return this;
    }

    /**
     * Sets the encoding configuration for the context.
     *
     * @param encodingConfiguration the encoding configuration to use
     * @return this builder
     */
    @Nonnull
    public Builder encodingConfiguration(
        @Nonnull final EncodingConfiguration encodingConfiguration) {
      this.encodingConfiguration = encodingConfiguration;
      return this;
    }

    /**
     * Sets the terminology configuration for the context.
     *
     * @param terminologyConfiguration the terminology configuration to use
     * @return this builder
     */
    @Nonnull
    public Builder terminologyConfiguration(
        @Nonnull final TerminologyConfiguration terminologyConfiguration) {
      this.terminologyConfiguration = terminologyConfiguration;
      return this;
    }

    /**
     * Sets the query configuration for the context.
     *
     * @param queryConfiguration the query configuration to use
     * @return this builder
     */
    @Nonnull
    public Builder queryConfiguration(@Nonnull final QueryConfiguration queryConfiguration) {
      this.queryConfiguration = queryConfiguration;
      return this;
    }

    /**
     * Builds a new {@link PathlingContext} instance with the configured options.
     *
     * @return a new {@link PathlingContext} instance
     */
    @Nonnull
    public PathlingContext build() {
      final SparkSession finalSpark = getOrDefault(spark, PathlingContext::buildDefaultSpark);
      final EncodingConfiguration finalEncodingConfig =
          getOrDefault(encodingConfiguration, EncodingConfiguration.builder()::build);
      final TerminologyConfiguration finalTerminologyConfig =
          getOrDefault(terminologyConfiguration, TerminologyConfiguration.builder()::build);
      final QueryConfiguration finalQueryConfig =
          getOrDefault(queryConfiguration, QueryConfiguration.builder()::build);

      validateConfigurations(finalEncodingConfig, finalTerminologyConfig, finalQueryConfig);

      return createContext(
          finalSpark, finalEncodingConfig, finalTerminologyConfig, finalQueryConfig);
    }

    @Nonnull
    private static <T> T getOrDefault(
        @Nullable final T value, @Nonnull final java.util.function.Supplier<T> defaultSupplier) {
      return value != null ? value : defaultSupplier.get();
    }

    private static void validateConfigurations(
        @Nonnull final EncodingConfiguration encodingConfig,
        @Nonnull final TerminologyConfiguration terminologyConfig,
        @Nonnull final QueryConfiguration queryConfig) {
      ValidationUtils.ensureValid(terminologyConfig, "Invalid terminology configuration");
      ValidationUtils.ensureValid(encodingConfig, "Invalid encoding configuration");
      ValidationUtils.ensureValid(queryConfig, "Invalid query configuration");
    }

    @Nonnull
    private static PathlingContext createContext(
        @Nonnull final SparkSession spark,
        @Nonnull final EncodingConfiguration encodingConfig,
        @Nonnull final TerminologyConfiguration terminologyConfig,
        @Nonnull final QueryConfiguration queryConfig) {
      final FhirEncoderBuilder encoderBuilder = getEncoderBuilder(encodingConfig);
      final TerminologyServiceFactory terminologyServiceFactory =
          getTerminologyServiceFactory(terminologyConfig);

      return new PathlingContext(
          spark, encoderBuilder.getOrCreate(), terminologyServiceFactory, queryConfig);
    }
  }

  /**
   * Creates a new {@link Builder} for building a {@link PathlingContext}.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a new {@link Builder} for building a {@link PathlingContext} with a pre-configured
   * Spark session.
   *
   * @param spark the Spark session to use, or null to use a default Spark session
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder(@Nullable final SparkSession spark) {
    return new Builder(spark);
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
   * Returns the encoding configuration used by this PathlingContext.
   *
   * <p>The configuration is constructed on-demand from the current state of the FhirEncoders
   * instance.
   *
   * @return the encoding configuration, never null
   */
  @Nonnull
  public EncodingConfiguration getEncodingConfiguration() {
    return fhirEncoders.getConfiguration();
  }

  /**
   * Returns the terminology configuration used by this PathlingContext.
   *
   * <p>The configuration is retrieved from the terminology service factory. Factories that do not
   * support configuration access will throw {@link IllegalStateException}.
   *
   * @return the terminology configuration, never null
   * @throws IllegalStateException if the terminology service factory does not support configuration
   *     access
   */
  @Nonnull
  public TerminologyConfiguration getTerminologyConfiguration() {
    return terminologyServiceFactory.getConfiguration();
  }

  /**
   * Creates a new {@link PathlingContext} with a default setup for Spark, FHIR encoders, and
   * terminology services.
   *
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext create() {
    return builder().build();
  }

  /**
   * Creates a new {@link PathlingContext} using a pre-configured {@link SparkSession}.
   *
   * @param sparkSession the Spark session to use
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext create(@Nonnull final SparkSession sparkSession) {
    return builder(sparkSession).build();
  }

  /**
   * Creates a new {@link PathlingContext} using supplied encoding configuration and a
   * pre-configured {@link SparkSession}.
   *
   * <p>This is a convenience method for case when only encoding functionality of Pathling is
   * needed.
   *
   * @param sparkSession the Spark session to use
   * @param encodingConfig the encoding configuration to use
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext createForEncoding(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final EncodingConfiguration encodingConfig) {
    return builder(sparkSession).encodingConfiguration(encodingConfig).build();
  }

  /**
   * Creates a new {@link PathlingContext} using supplied configuration terminology and a
   * pre-configured {@link SparkSession}.
   *
   * <p>This is a convenience method for case when only terminology functionality (terminology UDFs)
   * of Pathling is needed.
   *
   * @param sparkSession the Spark session to use
   * @param terminologyConfig the terminology configuration to use
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext createForTerminology(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final TerminologyConfiguration terminologyConfig) {
    return builder(sparkSession).terminologyConfiguration(terminologyConfig).build();
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
  public <T extends IBaseResource> Dataset<T> encode(
      @Nonnull final Dataset<String> stringResources,
      @Nonnull final Class<T> resourceClass,
      @Nonnull final String inputMimeType) {
    final ExpressionEncoder<T> encoder = fhirEncoders.of(resourceClass);
    return stringResources.mapPartitions(
        new EncodeResourceMapPartitions<>(fhirVersion, inputMimeType, resourceClass), encoder);
  }

  /**
   * Takes a dataframe with string representations of FHIR resources and encodes the resources of
   * the given type as a Spark dataframe.
   *
   * @param stringResourcesDf the dataframe with the string representation of the resources.
   * @param resourceName the name of the resources to encode.
   * @param inputMimeType the mime type of the encoding for the input strings.
   * @param maybeColumnName the name of the column in the input dataframe that contains the resource
   *     strings. If null the input dataframe must have a single column of type string.
   * @return the dataframe with Spark encoded resources.
   */
  @Nonnull
  public Dataset<Row> encode(
      @Nonnull final Dataset<Row> stringResourcesDf,
      @Nonnull final String resourceName,
      @Nonnull final String inputMimeType,
      @Nullable final String maybeColumnName) {

    final Dataset<String> stringResources =
        (nonNull(maybeColumnName) ? stringResourcesDf.select(maybeColumnName) : stringResourcesDf)
            .as(Encoders.STRING());

    final RuntimeResourceDefinition definition =
        FhirEncoders.contextFor(fhirVersion).getResourceDefinition(resourceName);
    return encode(stringResources, definition.getImplementingClass(), inputMimeType).toDF();
  }

  /**
   * Takes a dataframe with string representations of FHIR resources and encodes the resources of
   * the given type as a Spark dataframe.
   *
   * @param stringResourcesDf the dataframe with the string representation of the resources. The
   *     dataframe must have a single column of type string.
   * @param resourceName the name of the resources to encode.
   * @param inputMimeType the mime type of the encoding for the input strings.
   * @return the dataframe with Spark encoded resources.
   */
  @Nonnull
  public Dataset<Row> encode(
      @Nonnull final Dataset<Row> stringResourcesDf,
      @Nonnull final String resourceName,
      @Nonnull final String inputMimeType) {

    return encode(stringResourcesDf, resourceName, inputMimeType, null);
  }

  /**
   * Takes a dataframe with JSON representations of FHIR resources and encodes the resources of the
   * given type as a Spark dataframe.
   *
   * @param stringResourcesDf the dataframe with the JSON representation of the resources. The
   *     dataframe must have a single column of type string.
   * @param resourceName the name of the resources to encode.
   * @return the dataframe with Spark encoded resources.
   */
  @Nonnull
  public Dataset<Row> encode(
      @Nonnull final Dataset<Row> stringResourcesDf, @Nonnull final String resourceName) {
    return encode(stringResourcesDf, resourceName, FHIR_JSON);
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
  public <T extends IBaseResource> Dataset<String> decode(
      @Nonnull final Dataset<Row> resources,
      @Nonnull final String resourceName,
      @Nonnull final String outputMimeType) {
    final RuntimeResourceDefinition definition =
        FhirEncoders.contextFor(fhirVersion).getResourceDefinition(resourceName);

    @SuppressWarnings("unchecked")
    final Class<T> resourceClass = (Class<T>) definition.getImplementingClass();

    final ExpressionEncoder<T> encoder = fhirEncoders.of(resourceClass);
    final Dataset<T> typedResources = resources.as(encoder);

    final MapPartitionsFunction<T, String> mapper =
        new DecodeResourceMapPartitions<>(fhirVersion, outputMimeType);

    return typedResources.mapPartitions(mapper, Encoders.STRING());
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
      @Nonnull final Dataset<String> stringBundles,
      @Nonnull final Class<T> resourceClass,
      @Nonnull final String inputMimeType) {
    return stringBundles.mapPartitions(
        new EncodeBundleMapPartitions<>(fhirVersion, inputMimeType, resourceClass),
        fhirEncoders.of(resourceClass));
  }

  /**
   * Takes a dataframe with string representations of FHIR bundles and encodes the resources of the
   * given type as a Spark dataframe.
   *
   * @param stringBundlesDf the dataframe with the string representation of the resources
   * @param resourceName the name of the resources to encode
   * @param inputMimeType the MIME type of the input strings
   * @param maybeColumnName the name of the column in the input dataframe that contains the bundle
   *     strings. If null, the input dataframe must have a single column of type string.
   * @return a Spark dataframe containing the encoded resources
   */
  @Nonnull
  public Dataset<Row> encodeBundle(
      @Nonnull final Dataset<Row> stringBundlesDf,
      @Nonnull final String resourceName,
      @Nonnull final String inputMimeType,
      @Nullable final String maybeColumnName) {

    final Dataset<String> stringResources =
        (nonNull(maybeColumnName) ? stringBundlesDf.select(maybeColumnName) : stringBundlesDf)
            .as(Encoders.STRING());

    final RuntimeResourceDefinition definition =
        FhirEncoders.contextFor(fhirVersion).getResourceDefinition(resourceName);
    return encodeBundle(stringResources, definition.getImplementingClass(), inputMimeType).toDF();
  }

  /**
   * Takes a dataframe with string representations of FHIR bundles and encodes the resources of the
   * given type as a Spark dataframe.
   *
   * @param stringBundlesDf the dataframe with the string representation of the bundles. The
   *     dataframe must have a single column of type string.
   * @param resourceName the name of the resources to encode
   * @param inputMimeType the MIME type of the input strings
   * @return a Spark dataframe containing the encoded resources
   */
  @Nonnull
  public Dataset<Row> encodeBundle(
      @Nonnull final Dataset<Row> stringBundlesDf,
      @Nonnull final String resourceName,
      @Nonnull final String inputMimeType) {
    return encodeBundle(stringBundlesDf, resourceName, inputMimeType, null);
  }

  /**
   * Takes a dataframe with JSON representations of FHIR bundles and encodes the resources of the
   * given type as a Spark dataframe.
   *
   * @param stringBundlesDf the dataframe with the JSON representation of the resources. The
   *     dataframe must have a single column of type string.
   * @param resourceName the name of the resources to encode
   * @return a Spark dataframe containing the encoded resources
   */
  @Nonnull
  public Dataset<Row> encodeBundle(
      @Nonnull final Dataset<Row> stringBundlesDf, @Nonnull final String resourceName) {
    return encodeBundle(stringBundlesDf, resourceName, FHIR_JSON);
  }

  /**
   * Creates a new DataSourceBuilder for reading FHIR data from various sources.
   *
   * @return a new {@link DataSourceBuilder} that can be used to read from a variety of different
   *     data sources
   */
  @Nonnull
  public DataSourceBuilder read() {
    return new DataSourceBuilder(this);
  }

  /**
   * Gets the version of the Pathling library.
   *
   * @return the version of the Pathling library
   */
  @Nonnull
  public String getVersion() {
    return new PathlingVersion().getDescriptiveVersion().orElse("UNKNOWN");
  }

  /**
   * Checks if the given resource type is supported by the Pathling context.
   *
   * @param resourceType the resource type to check
   * @return true if the resource type is supported, false otherwise
   */
  public boolean isResourceTypeSupported(@Nonnull final String resourceType) {
    return ResourceTypes.isSupported(resourceType);
  }

  /**
   * Matches the given string against supported resource types in a case-insensitive fashion.
   *
   * @param resourceTypeString the string to match against resource types
   * @return an Optional containing the resource type code if the string matches a supported
   *     resource type, empty otherwise
   */
  @Nonnull
  public Optional<String> matchSupportedResourceType(@Nonnull final String resourceTypeString) {
    return ResourceTypes.matchSupportedResourceType(resourceTypeString);
  }

  /**
   * Converts a FHIR search expression to a boolean filter column.
   *
   * <p>This method takes a FHIR search query string and returns a Spark Column that can be used to
   * filter a DataFrame of FHIR resources. The resulting Column evaluates to true for resources that
   * match the search criteria.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * PathlingContext pc = PathlingContext.create(spark);
   *
   * // Single parameter
   * Column genderFilter = pc.searchToColumn("Patient", "gender=male");
   *
   * // Multiple parameters (AND)
   * Column combinedFilter = pc.searchToColumn("Patient", "gender=male&birthdate=ge1990-01-01");
   *
   * // Apply to DataFrame
   * Dataset<Row> patients = dataSource.read("Patient");
   * Dataset<Row> filtered = patients.filter(combinedFilter);
   * }</pre>
   *
   * @param resourceType the FHIR resource type (e.g., "Patient", "Observation")
   * @param searchExpression URL query string format (e.g., "gender=male&amp;birthdate=ge1990")
   * @return a Spark Column representing the boolean filter condition
   * @throws IllegalArgumentException if the search expression contains unknown parameters
   */
  @Nonnull
  public Column searchToColumn(
      @Nonnull final String resourceType, @Nonnull final String searchExpression) {
    final SearchColumnBuilder builder = SearchColumnBuilder.withDefaultRegistry(getFhirContext());
    return builder.fromQueryString(ResourceType.fromCode(resourceType), searchExpression);
  }

  /**
   * Converts a FHIRPath expression to a Spark Column.
   *
   * <p>This method takes a FHIRPath expression and returns a Spark Column that can be used in
   * DataFrame operations such as {@code filter()} and {@code select()}. The expression is evaluated
   * against the specified resource type using the existing FHIRPath engine.
   *
   * <p>The expression should evaluate to a single value per resource row. Boolean expressions can
   * be used for filtering, while other expressions can be used for value extraction.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * PathlingContext pc = PathlingContext.create(spark);
   *
   * // Boolean expression for filtering
   * Column genderFilter = pc.fhirPathToColumn("Patient", "gender = 'male'");
   * Dataset<Row> filtered = patients.filter(genderFilter);
   *
   * // Value expression for selection
   * Column nameColumn = pc.fhirPathToColumn("Patient", "name.given.first()");
   * Dataset<Row> names = patients.select(nameColumn);
   * }</pre>
   *
   * @param resourceType the FHIR resource type (e.g., "Patient", "Observation")
   * @param fhirPathExpression the FHIRPath expression to evaluate
   * @return a Spark Column representing the evaluated expression
   * @throws IllegalArgumentException if the resource type is invalid
   */
  @Nonnull
  public Column fhirPathToColumn(
      @Nonnull final String resourceType, @Nonnull final String fhirPathExpression) {
    final SearchColumnBuilder builder = SearchColumnBuilder.withDefaultRegistry(getFhirContext());
    return builder.fromExpression(ResourceType.fromCode(resourceType), fhirPathExpression);
  }

  /**
   * Evaluates a FHIRPath expression against a single FHIR resource and returns materialised typed
   * results.
   *
   * <p>The resource is encoded into a one-row Spark Dataset internally, and the existing FHIRPath
   * engine is used to evaluate the expression. Results are collected and returned as typed values.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * PathlingContext pc = PathlingContext.create(spark);
   * FhirPathResult result = pc.evaluateFhirPath("Patient", patientJson, "name.family");
   * for (FhirPathResult.TypedValue value : result.getResults()) {
   *     System.out.println(value.getType() + ": " + value.getValue());
   * }
   * }</pre>
   *
   * @param resourceType the FHIR resource type (e.g., "Patient", "Observation")
   * @param resourceJson the FHIR resource as a JSON string
   * @param fhirPathExpression the FHIRPath expression to evaluate
   * @return a {@link FhirPathResult} containing typed result values and type metadata
   * @throws IllegalArgumentException if the resource type is invalid
   */
  @Nonnull
  public FhirPathResult evaluateFhirPath(
      @Nonnull final String resourceType,
      @Nonnull final String resourceJson,
      @Nonnull final String fhirPathExpression) {
    return evaluateFhirPath(resourceType, resourceJson, fhirPathExpression, null, null);
  }

  /**
   * Evaluates a FHIRPath expression against a single FHIR resource with an optional context
   * expression and variables.
   *
   * <p>When a context expression is provided, the context expression is evaluated first. The main
   * expression is then evaluated once for each item in the context result, with each context item
   * used as the input. Results are returned as a flat list of typed values.
   *
   * @param resourceType the FHIR resource type (e.g., "Patient", "Observation")
   * @param resourceJson the FHIR resource as a JSON string
   * @param fhirPathExpression the FHIRPath expression to evaluate
   * @param contextExpression an optional context expression; if non-null, the main expression is
   *     evaluated once per context item
   * @param variables optional named variables available via %variable syntax, or null
   * @return a {@link FhirPathResult} containing typed result values and type metadata
   * @throws IllegalArgumentException if the resource type is invalid
   */
  @Nonnull
  public FhirPathResult evaluateFhirPath(
      @Nonnull final String resourceType,
      @Nonnull final String resourceJson,
      @Nonnull final String fhirPathExpression,
      @Nullable final String contextExpression,
      @Nullable final Map<String, String> variables) {

    // Encode the resource JSON into a one-row Spark Dataset.
    final Dataset<Row> resourceDf = encodeResourceJson(resourceType, resourceJson);

    // Parse the FHIRPath expression.
    final Parser parser = new Parser();
    final FhirPath mainPath = parser.parse(fhirPathExpression);

    // Create a single resource evaluator for determining the return type.
    final SingleResourceEvaluator evaluator =
        SingleResourceEvaluatorBuilder.create(
                org.hl7.fhir.r4.model.Enumerations.ResourceType.fromCode(resourceType),
                getFhirContext())
            .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
            .build();

    // Evaluate to determine the return type.
    final Collection resultCollection = evaluator.evaluate(mainPath);
    final String expectedReturnType = determineReturnType(resultCollection);

    if (contextExpression != null) {
      return evaluateWithContext(
          resourceDf, parser, mainPath, contextExpression, evaluator, expectedReturnType);
    }

    // Apply the result Column to the dataset and collect the results.
    final Column resultColumn = resultCollection.getColumn().getValue();
    final List<TypedValue> results = collectResults(resourceDf, resultColumn, resultCollection);
    return new FhirPathResult(results, expectedReturnType);
  }

  /**
   * Evaluates the main expression once per context item, returning flat results.
   *
   * @param resourceDf the encoded resource as a single-row DataFrame
   * @param parser the FHIRPath parser
   * @param mainPath the parsed main expression
   * @param contextExpression the context expression string
   * @param evaluator the single resource evaluator
   * @param expectedReturnType the inferred return type of the main expression
   * @return a FhirPathResult with results for each context item
   */
  @Nonnull
  private FhirPathResult evaluateWithContext(
      @Nonnull final Dataset<Row> resourceDf,
      @Nonnull final Parser parser,
      @Nonnull final FhirPath mainPath,
      @Nonnull final String contextExpression,
      @Nonnull final SingleResourceEvaluator evaluator,
      @Nonnull final String expectedReturnType) {

    // Parse and evaluate the context expression.
    final FhirPath contextPath = parser.parse(contextExpression);
    final Collection contextCollection = evaluator.evaluate(contextPath);
    final Column contextColumn = contextCollection.getColumn().getValue();

    // Collect context items.
    final Dataset<Row> contextDf = resourceDf.select(contextColumn.alias("_ctx"));
    final List<Row> contextRows = contextDf.collectAsList();

    if (contextRows.isEmpty() || contextRows.get(0).isNullAt(0)) {
      return new FhirPathResult(Collections.emptyList(), expectedReturnType);
    }

    // The context value is an array; evaluate the main expression against the input context
    // for each item. In flat schema mode, the evaluator uses the context expression to scope
    // the main expression, so we compose the expressions.
    final FhirPath composedPath = contextPath.andThen(mainPath);
    final Collection composedResult = evaluator.evaluate(composedPath);
    final Column composedColumn = composedResult.getColumn().getValue();

    final List<TypedValue> results = collectResults(resourceDf, composedColumn, composedResult);
    return new FhirPathResult(results, expectedReturnType);
  }

  /**
   * Encodes a single FHIR resource JSON string into a one-row Spark Dataset.
   *
   * @param resourceType the FHIR resource type
   * @param resourceJson the JSON string
   * @return a single-row Dataset representing the encoded resource
   */
  @Nonnull
  private Dataset<Row> encodeResourceJson(
      @Nonnull final String resourceType, @Nonnull final String resourceJson) {
    final Dataset<Row> jsonDf =
        spark.createDataset(Collections.singletonList(resourceJson), Encoders.STRING()).toDF();
    return encode(jsonDf, resourceType, FHIR_JSON);
  }

  /**
   * Determines the return type from a Collection's type information.
   *
   * @param collection the collection to inspect
   * @return the type name as a string
   */
  @Nonnull
  private static String determineReturnType(@Nonnull final Collection collection) {
    // Prefer the FHIR defined type code (e.g., "HumanName", "code", "string").
    return collection
        .getFhirType()
        .map(org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType::toCode)
        .orElseGet(
            () -> collection.getType().map(FhirPathType::getTypeSpecifier).orElse("unknown"));
  }

  /**
   * Collects materialised results from a Dataset by applying a result Column.
   *
   * @param resourceDf the single-row encoded resource Dataset
   * @param resultColumn the Column expression for the result
   * @param collection the Collection with type metadata
   * @return a list of typed values
   */
  @Nonnull
  private static List<TypedValue> collectResults(
      @Nonnull final Dataset<Row> resourceDf,
      @Nonnull final Column resultColumn,
      @Nonnull final Collection collection) {

    final String typeName = determineReturnType(collection);
    final Dataset<Row> resultDf = resourceDf.select(resultColumn.alias("_result"));
    final List<Row> rows = resultDf.collectAsList();

    if (rows.isEmpty()) {
      return Collections.emptyList();
    }

    final Row row = rows.get(0);
    if (row.isNullAt(0)) {
      return Collections.emptyList();
    }

    final Object rawValue = row.get(0);
    return materialiseValues(rawValue, typeName);
  }

  /**
   * Materialises a raw Spark value into a list of TypedValue objects.
   *
   * <p>Array values are expanded into individual typed values. Complex struct values are serialised
   * as JSON strings.
   *
   * @param rawValue the raw value from Spark
   * @param typeName the FHIR type name
   * @return a list of typed values
   */
  @Nonnull
  private static List<TypedValue> materialiseValues(
      @Nonnull final Object rawValue, @Nonnull final String typeName) {
    final List<TypedValue> results = new ArrayList<>();

    if (rawValue instanceof scala.collection.Seq<?> seq) {
      // Array/list result - expand each element.
      for (int i = 0; i < seq.size(); i++) {
        final Object element = seq.apply(i);
        if (element != null) {
          results.add(new TypedValue(typeName, convertValue(element, typeName)));
        }
      }
    } else {
      // Singular result.
      results.add(new TypedValue(typeName, convertValue(rawValue, typeName)));
    }
    return results;
  }

  /**
   * Converts a raw Spark value to a Java value suitable for the result.
   *
   * <p>Struct types (complex FHIR types) are converted to JSON strings. Primitive types are
   * returned as-is.
   *
   * @param value the raw value
   * @param typeName the FHIR type name
   * @return the converted value
   */
  @Nonnull
  private static Object convertValue(@Nonnull final Object value, @Nonnull final String typeName) {
    if (value instanceof Row row) {
      // Complex type: convert to JSON string representation.
      return rowToJson(row);
    }
    return value;
  }

  /**
   * Converts a Spark Row to a JSON string representation.
   *
   * @param row the row to convert
   * @return a JSON string
   */
  @Nonnull
  private static String rowToJson(@Nonnull final Row row) {
    return row.json();
  }

  @Nonnull
  private static SparkSession buildDefaultSpark() {
    return SparkSession.builder().appName("Pathling").master("local[*]").getOrCreate();
  }

  @Nonnull
  private static FhirEncoderBuilder getEncoderBuilder(@Nonnull final EncodingConfiguration config) {
    return FhirEncoders.forR4()
        .withMaxNestingLevel(config.getMaxNestingLevel())
        .withExtensionsEnabled(config.isEnableExtensions())
        .withOpenTypes(config.getOpenTypes());
  }

  @Nonnull
  private static TerminologyServiceFactory getTerminologyServiceFactory(
      @Nonnull final TerminologyConfiguration configuration) {
    // Pathling only supports FHIR R4, so we use the version enum directly to avoid creating another
    // FhirContext.
    return new DefaultTerminologyServiceFactory(FhirVersionEnum.R4, configuration);
  }
}
