package au.csiro.pathling.api;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.support.FhirConversionSupport;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.nonNull;

/**
 * Main entry point to Pathling Java API
 */
public class PathlingContext {

    @Nonnull
    private final SparkSession sparkSession;

    @Nonnull
    private final FhirVersionEnum fhirVersion;

    @Nonnull
    private final FhirEncoders fhirEncoders;

    private PathlingContext(@Nonnull final SparkSession sparkSession, @Nonnull final FhirEncoders fhirEncoders) {
        this.sparkSession = sparkSession;
        this.fhirVersion = fhirEncoders.getFhirVersion();
        this.fhirEncoders = fhirEncoders;
    }

    /**
     * Creates new instance of PathlingContext for given SparkSession and FhirEncoders.
     *
     * @param sparkSession the SparkSession to use.
     * @param fhirEncoders the FhirEncoders to use.
     * @return then new instance of PathlingContext.
     */
    public static PathlingContext of(@Nonnull final SparkSession sparkSession, @Nonnull final FhirEncoders fhirEncoders) {
        return new PathlingContext(sparkSession, fhirEncoders);
    }

    /**
     * Creates new instance of PathlingContext for given SparkSession and Fhir R4 encoder with default settings.
     *
     * @param sparkSession the SparkSession to use.
     * @return then new instance of PathlingContext.
     */
    public static PathlingContext of(@Nonnull final SparkSession sparkSession) {
        return new PathlingContext(sparkSession, FhirEncoders.forR4().getOrCreate());
    }

    /**
     * Creates new instance of PathlingContext for given SparkSession, FHIR version and encoder settings.
     * The null setting values default to the default encoders setting.
     *
     * @param sparkSession     the SparkSession to use.
     * @param versionString    the FHIR version to use. Must be a valid FHIR version string. If null defaults to 'R4'.
     * @param maxNestingLevel  the maximum nesting level for recursive data types. Zero (0) indicates
     *                         that all direct or indirect fields of type T in element of type T should be skipped.
     * @param enableExtensions switches on/off the support for FHIR extensions.
     * @param enabledOpenTypes list of types that are encoded within open types, such as extensions.
     * @return then new instance of PathlingContext.
     */
    public static PathlingContext of(@Nonnull final SparkSession sparkSession, @Nullable final String versionString,
                                     @Nullable final Integer maxNestingLevel, @Nullable final Boolean enableExtensions,
                                     @Nullable final List<String> enabledOpenTypes) {

        FhirEncoders.Builder encoderBuilder = nonNull(versionString) ?
                FhirEncoders.forVersion(FhirVersionEnum.forVersionString(versionString)) :
                FhirEncoders.forR4();
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
        return of(sparkSession, encoderBuilder.getOrCreate());
    }

    static class EncodeResourceMapPartitionsFunc<T extends IBaseResource> extends EncodeMapPartitionsFunc<T> {

        private static final long serialVersionUID = 6405663463302424287L;

        EncodeResourceMapPartitionsFunc(FhirVersionEnum fhirVersion, final String inputMimeType,
                                        Class<T> resourceClass) {
            super(fhirVersion, inputMimeType, resourceClass);
        }

        @Nonnull
        @Override
        protected Stream<IBaseResource> processResources(@Nonnull Stream<IBaseResource> resources) {
            return resources.filter(resourceClass::isInstance);
        }
    }

    /**
     * Takes a dataset with string representations of FHIR resources and encodes
     * the resources of the given type as a Spark dataset.
     *
     * @param stringResources the dataset with the string representation of the resources.
     * @param resourceClass   the class of the resources to encode.
     * @param inputMimeType   the mime type of the encoding for the input strings.
     * @param <T>             the Java type of the resource
     * @return the dataset with Spark encoded resources.
     */
    @Nonnull
    public <T extends IBaseResource> Dataset<T> encode(@Nonnull final Dataset<String> stringResources,
                                                       @Nonnull final Class<T> resourceClass,
                                                       @Nonnull final String inputMimeType
    ) {
        return stringResources.mapPartitions(
                new EncodeResourceMapPartitionsFunc<>(fhirVersion, inputMimeType, resourceClass),
                fhirEncoders.of(resourceClass));
    }


    /**
     * Takes a dataframe with string representations of FHIR resources and encodes
     * the resources of the given type as a Spark dataframe.
     *
     * @param stringResourcesDF the dataframe with the string representation of the resources.
     * @param resourceName      the name of the resources to encode.
     * @param inputMimeType     the mime type of the encoding for the input strings.
     * @param maybeColumnName   the name of the column in the input dataframe that contains the resource strings.
     *                          If null the input dataframe must have a single column of type string.
     * @return the dataframe with Spark encoded resources.
     */
    @Nonnull
    public Dataset<Row> encode(@Nonnull final Dataset<Row> stringResourcesDF,
                               @Nonnull final String resourceName,
                               @Nonnull final String inputMimeType,
                               @Nullable final String maybeColumnName) {

        final Dataset<String> stringResources = (nonNull(maybeColumnName) ?
                stringResourcesDF.select(maybeColumnName) : stringResourcesDF).as(Encoders.STRING());

        RuntimeResourceDefinition definition = FhirEncoders.contextFor(fhirVersion)
                .getResourceDefinition(resourceName);
        return encode(stringResources, definition.getImplementingClass(), inputMimeType).toDF();
    }

    /**
     * Takes a dataframe with string representations of FHIR resources and encodes
     * the resources of the given type as a Spark dataframe.
     *
     * @param stringResourcesDF the dataframe with the string representation of the resources.
     *                          The dataframe must have a single column of type string.
     * @param resourceName      the name of the resources to encode.
     * @param inputMimeType     the mime type of the encoding for the input strings.
     * @return the dataframe with Spark encoded resources.
     */
    @Nonnull
    public Dataset<Row> encode(@Nonnull final Dataset<Row> stringResourcesDF,
                               @Nonnull final String resourceName,
                               @Nonnull final String inputMimeType) {

        return encode(stringResourcesDF, resourceName, inputMimeType, null);
    }

    /**
     * Takes a dataframe with JSON representations of FHIR resources and encodes
     * the resources of the given type as a Spark dataframe.
     *
     * @param stringResourcesDF the dataframe with the JSON representation of the resources.
     *                          The dataframe must have a single column of type string.
     * @param resourceName      the name of the resources to encode.
     * @return the dataframe with Spark encoded resources.
     */
    @Nonnull
    public Dataset<Row> encode(@Nonnull final Dataset<Row> stringResourcesDF,
                               @Nonnull final String resourceName) {
        return encode(stringResourcesDF, resourceName, FhirMimeTypes.FHIR_JSON);
    }


    static class EncodeBundleMapPartitionsFunc<T extends IBaseResource> extends EncodeMapPartitionsFunc<T> {

        private static final long serialVersionUID = -4264073360143318480L;

        EncodeBundleMapPartitionsFunc(FhirVersionEnum fhirVersion, String inputMimeType, Class<T> resourceClass) {
            super(fhirVersion, inputMimeType, resourceClass);
        }

        @Nonnull
        @Override
        protected Stream<IBaseResource> processResources(@Nonnull Stream<IBaseResource> resources) {
            final FhirConversionSupport conversionSupport = FhirConversionSupport.supportFor(fhirVersion);
            return resources.flatMap(maybeBundle -> conversionSupport.extractEntryFromBundle((IBaseBundle) maybeBundle,
                    resourceClass).stream());
        }
    }


    /**
     * Takes a dataset with string representations of FHIR bundles and encodes
     * the resources of the given type as a Spark dataset.
     *
     * @param stringBundles the dataset with the string representation of the resources.
     * @param resourceClass the class of the resources to encode.
     * @param inputMimeType the mime type of the encoding for the input strings.
     * @param <T>           the Java type of the resource
     * @return the dataset with Spark encoded resources.
     */
    @Nonnull
    public <T extends IBaseResource> Dataset<T> encodeBundle(@Nonnull final Dataset<String> stringBundles,
                                                             @Nonnull final Class<T> resourceClass,
                                                             @Nonnull final String inputMimeType) {
        return stringBundles.mapPartitions(
                new EncodeBundleMapPartitionsFunc<>(fhirVersion, inputMimeType, resourceClass),
                fhirEncoders.of(resourceClass));
    }

    /**
     * Takes a dataframe with string representations of FHIR bundles and encodes
     * the resources of the given type as a Spark dataframe.
     *
     * @param stringBundlesDF the dataframe with the string representation of the resources.
     * @param resourceName    the name of the resources to encode.
     * @param inputMimeType   the mime type of the encoding for the input strings.
     * @param maybeColumnName the name of the column in the input dataframe that contains the bundle strings.
     *                        If null the input dataframe must have a single column of type string.
     * @return the dataframe with Spark encoded resources.
     */
    @Nonnull
    public Dataset<Row> encodeBundle(@Nonnull final Dataset<Row> stringBundlesDF,
                                     @Nonnull final String resourceName,
                                     @Nonnull final String inputMimeType,
                                     @Nullable final String maybeColumnName) {

        final Dataset<String> stringResources = (nonNull(maybeColumnName) ?
                stringBundlesDF.select(maybeColumnName) : stringBundlesDF).as(Encoders.STRING());

        RuntimeResourceDefinition definition = FhirEncoders.contextFor(fhirVersion)
                .getResourceDefinition(resourceName);
        return encodeBundle(stringResources, definition.getImplementingClass(), inputMimeType).toDF();
    }

    /**
     * Takes a dataframe with string representations of FHIR bundles and encodes
     * the resources of the given type as a Spark dataframe.
     *
     * @param stringBundlesDF the dataframe with the string representation of the bundles.
     *                        The dataframe must have a single column of type string.
     * @param resourceName    the name of the resources to encode.
     * @param inputMimeType   the mime type of the encoding for the input strings.
     * @return the dataframe with Spark encoded resources.
     */
    @Nonnull
    public Dataset<Row> encodeBundle(@Nonnull final Dataset<Row> stringBundlesDF,
                                     @Nonnull final String resourceName,
                                     @Nonnull final String inputMimeType) {
        return encodeBundle(stringBundlesDF, resourceName, inputMimeType, null);
    }


    /**
     * Takes a dataframe with JSON representations of FHIR bundles and encodes
     * the resources of the given type as a Spark dataframe.
     *
     * @param stringBundlesDF the dataframe with the JSON representation of the resources.
     *                        The dataframe must have a single column of type string.
     * @param resourceName    the name of the resources to encode.
     * @return the dataframe with Spark encoded resources.
     */
    @Nonnull
    public Dataset<Row> encodeBundle(@Nonnull final Dataset<Row> stringBundlesDF,
                                     @Nonnull final String resourceName) {
        return encodeBundle(stringBundlesDF, resourceName, FhirMimeTypes.FHIR_JSON);
    }

}
