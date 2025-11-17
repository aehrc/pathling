/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.encoders.FhirEncoderBuilder;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
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
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.exceptions.FHIRException;
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
  @Getter
  private final QueryConfiguration queryConfiguration;

  @Nonnull
  @Getter
  private final Gson gson;

  /**
   * Creates a new PathlingContext with the specified configuration.
   *
   * @param spark the Spark session to use
   * @param fhirEncoders the FHIR encoders to use
   * @param terminologyServiceFactory the terminology service factory to use
   * @param queryConfiguration the query configuration to use
   */
  private PathlingContext(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final QueryConfiguration queryConfiguration) {
    this.spark = spark;
    this.fhirVersion = fhirEncoders.getFhirVersion();
    this.fhirEncoders = fhirEncoders;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.queryConfiguration = queryConfiguration;
    TerminologyUdfRegistrar.registerUdfs(spark, terminologyServiceFactory);
    PathlingUdfConfigurer.registerUDFs(spark);
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
  public static PathlingContext createInternal(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    return new PathlingContext(spark, fhirEncoders, terminologyServiceFactory,
        QueryConfiguration.builder().build());
  }

  /**
   * Builder for creating {@link PathlingContext} instances with configurable options.
   */
  public static class Builder {

    @Nullable
    private SparkSession spark;
    @Nullable
    private EncodingConfiguration encodingConfiguration;
    @Nullable
    private TerminologyConfiguration terminologyConfiguration;
    @Nullable
    private QueryConfiguration queryConfiguration;

    Builder() {
    }

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
      final EncodingConfiguration finalEncodingConfig = getOrDefault(encodingConfiguration,
          EncodingConfiguration.builder()::build);
      final TerminologyConfiguration finalTerminologyConfig = getOrDefault(
          terminologyConfiguration, TerminologyConfiguration.builder()::build);
      final QueryConfiguration finalQueryConfig = getOrDefault(queryConfiguration,
          QueryConfiguration.builder()::build);

      validateConfigurations(finalEncodingConfig, finalTerminologyConfig, finalQueryConfig);

      return createContext(finalSpark, finalEncodingConfig, finalTerminologyConfig,
          finalQueryConfig);
    }

    @Nonnull
    private static <T> T getOrDefault(@Nullable final T value,
        @Nonnull final java.util.function.Supplier<T> defaultSupplier) {
      return value != null
             ? value
             : defaultSupplier.get();
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
    private static PathlingContext createContext(@Nonnull final SparkSession spark,
        @Nonnull final EncodingConfiguration encodingConfig,
        @Nonnull final TerminologyConfiguration terminologyConfig,
        @Nonnull final QueryConfiguration queryConfig) {
      final FhirEncoderBuilder encoderBuilder = getEncoderBuilder(encodingConfig);
      final TerminologyServiceFactory terminologyServiceFactory =
          getTerminologyServiceFactory(terminologyConfig);

      return new PathlingContext(spark, encoderBuilder.getOrCreate(),
          terminologyServiceFactory, queryConfig);
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
   * <p>
   * The configuration is constructed on-demand from the current state of the FhirEncoders
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
   * <p>
   * The configuration is retrieved from the terminology service factory. Factories that do not
   * support configuration access will throw {@link IllegalStateException}.
   *
   * @return the terminology configuration, never null
   * @throws IllegalStateException if the terminology service factory does not support configuration
   * access
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
   * Creates a new {@link PathlingContext} using supplied encodinf configuration and a
   * pre-configured {@link SparkSession}.
   * <p>
   * This is a convenience method for case when only encoding functionality of Pathling is needed.
   *
   * @param sparkSession the Spark session to use
   * @param encodingConfig the encoding configuration to use
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext createForEncoding(@Nonnull final SparkSession sparkSession,
      @Nonnull final EncodingConfiguration encodingConfig) {
    return builder(sparkSession)
        .encodingConfiguration(encodingConfig)
        .build();
  }

  /**
   * Creates a new {@link PathlingContext} using supplied configuration terminology and a
   * pre-configured {@link SparkSession}.
   * <p>
   * This is a convenience method for case when only terminology functionality (terminology UDFs) of
   * Pathling is needed.
   *
   * @param sparkSession the Spark session to use
   * @param terminologyConfig the terminology configuration to use
   * @return a new {@link PathlingContext} instance
   */
  @Nonnull
  public static PathlingContext createForTerminology(@Nonnull final SparkSession sparkSession,
      @Nonnull final TerminologyConfiguration terminologyConfig) {
    return builder(sparkSession)
        .terminologyConfiguration(terminologyConfig)
        .build();
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
    return encode(stringResourcesDF, resourceName, FHIR_JSON);
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
    return encodeBundle(stringBundlesDF, resourceName, FHIR_JSON);
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

  /**
   * Checks if the given resource type is supported by the Pathling context.
   *
   * @param resourceType the resource type to check
   * @return true if the resource type is supported, false otherwise
   */
  public boolean isResourceTypeSupported(@Nonnull final String resourceType) {
    if (EncoderBuilder.UNSUPPORTED_RESOURCES().contains(resourceType)) {
      return false;
    }
    try {
      final ResourceType match = ResourceType.fromCode(resourceType);
      return match != null;
    } catch (final FHIRException e) {
      return false;
    }
  }

  /**
   * Matches the given string against supported resource types in a case-insensitive fashion.
   *
   * @param resourceTypeString the string to match against resource types
   * @return an Optional containing the resource type code if the string matches a supported
   * resource type, empty otherwise
   */
  @Nonnull
  public Optional<String> matchSupportedResourceType(@Nonnull final String resourceTypeString) {
    if (EncoderBuilder.UNSUPPORTED_RESOURCES().contains(resourceTypeString)) {
      return Optional.empty();
    }

    try {
      // Try exact match first.
      final ResourceType exactMatch = ResourceType.fromCode(resourceTypeString);
      if (exactMatch != null) {
        return Optional.of(exactMatch.toCode());
      }
    } catch (final FHIRException ignored) {
      // Continue to case-insensitive search
    }

    // Try case-insensitive match.
    for (final ResourceType resourceType : ResourceType.values()) {
      if (resourceTypeString.equalsIgnoreCase(resourceType.toCode()) &&
          !EncoderBuilder.UNSUPPORTED_RESOURCES().contains(resourceType.toCode())) {
        return Optional.ofNullable(resourceType.toCode());
      }
    }

    return Optional.empty();
  }

  @Nonnull
  private static SparkSession buildDefaultSpark() {
    return SparkSession.builder()
        .appName("Pathling")
        .master("local[*]")
        .getOrCreate();
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
    final FhirVersionEnum fhirVersion = FhirContext.forR4().getVersion().getVersion();
    return new DefaultTerminologyServiceFactory(fhirVersion, configuration);
  }

}
