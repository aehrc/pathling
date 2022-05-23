package au.csiro.pathling.api;

import au.csiro.pathling.api.definitions.FhirConversionSupport;
import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.nonNull;

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

    public static PathlingContext of(@Nonnull final SparkSession sparkSession, @Nonnull final FhirEncoders fhirEncoders) {
        return new PathlingContext(sparkSession, fhirEncoders);
    }

    public static PathlingContext of(@Nonnull final SparkSession sparkSession) {
        return new PathlingContext(sparkSession, FhirEncoders.forR4().getOrCreate());
    }


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

    static class EncodeResourceMapPartitionsFunc<T extends IBaseResource> implements MapPartitionsFunction<String, T> {

        private static final long serialVersionUID = 6405663463302424287L;
        private final FhirVersionEnum fhirVersion;
        private final String inputMimeType;
        private final Class<T> resourceClass;

        EncodeResourceMapPartitionsFunc(FhirVersionEnum fhirVersion, final String inputMimeType,
                                        Class<T> resourceClass) {
            this.fhirVersion = fhirVersion;
            this.inputMimeType = inputMimeType;
            this.resourceClass = resourceClass;
        }

        @Override
        public Iterator<T> call(Iterator<String> iterator) {
            final IParser parser = FhirParserFactory.forVersion(fhirVersion).createParser(inputMimeType);

            final Iterable<String> iterable = () -> iterator;
            Iterator<IBaseResource> resourceIt = StreamSupport.stream(iterable.spliterator(), false)
                    .map(parser::parseResource)
                    .filter(resourceClass::isInstance)
                    .iterator();
            //noinspection unchecked
            return (Iterator<T>) resourceIt;
        }
    }


    @Nonnull
    public <T extends IBaseResource> Dataset<T> encode(@Nonnull final Dataset<String> stringResources,
                                                       @Nonnull final Class<T> resourceClass,
                                                       @Nonnull final String inputMimeType
    ) {
        return stringResources.mapPartitions(
                new EncodeResourceMapPartitionsFunc<>(fhirVersion, inputMimeType, resourceClass),
                fhirEncoders.of(resourceClass));
    }

    @Nonnull
    public Dataset<Row> encode(@Nonnull final Dataset<String> stringResources,
                               @Nonnull final String resourceName,
                               @Nonnull final String inputMimeType) {

        RuntimeResourceDefinition definition = FhirEncoders.contextFor(fhirVersion)
                .getResourceDefinition(resourceName);
        return encode(stringResources, definition.getImplementingClass(), inputMimeType).toDF();
    }

    @Nonnull
    public Dataset<Row> pyEncode(@Nonnull final Dataset<Row> stringResourcesDF,
                                 @Nonnull final String resourceName,
                                 @Nullable final String maybeInputMimeType,
                                 @Nullable final String maybeColumnName) {

        final Dataset<String> stringResources = stringResourcesDF.select(nonNull(maybeColumnName) ? maybeColumnName : "value").as(Encoders.STRING());
        return encode(
                stringResources,
                resourceName,
                nonNull(maybeInputMimeType) ? maybeInputMimeType : FhirMimeTypes.FHIR_JSON);

    }


    static class EncodeBundleMapPartitionsFunc<T extends IBaseResource> implements MapPartitionsFunction<String, T> {

        private static final long serialVersionUID = -4264073360143318480L;
        private final FhirVersionEnum fhirVersion;
        private final Class<T> resourceClass;
        private final String inputMimeType;

        EncodeBundleMapPartitionsFunc(FhirVersionEnum fhirVersion, Class<T> resourceClass, String inputMimeType) {
            this.fhirVersion = fhirVersion;
            this.resourceClass = resourceClass;
            this.inputMimeType = inputMimeType;
        }

        @Override
        public Iterator<T> call(Iterator<String> iterator) {
            final IParser parser = FhirParserFactory.forVersion(fhirVersion).createParser(inputMimeType);
            final FhirConversionSupport conversionSupport = FhirConversionSupport.supportFor(fhirVersion);

            final Iterable<String> iterable = () -> iterator;
            Iterator<IBaseResource> resourceIt = StreamSupport.stream(iterable.spliterator(), false)
                    .map(parser::parseResource)
                    .flatMap(maybeBundle -> conversionSupport.extractEntryFromBundle((IBaseBundle) maybeBundle,
                            resourceClass).stream())
                    .iterator();
            //noinspection unchecked
            return (Iterator<T>) resourceIt;
        }

    }

    @Nonnull
    public <T extends IBaseResource> Dataset<T> encodeBundle(@Nonnull final Dataset<String> stringResources,
                                                             @Nonnull final Class<T> resourceClass,
                                                             @Nonnull final String inputMimeType) {
        return stringResources.mapPartitions(
                new EncodeBundleMapPartitionsFunc<>(fhirVersion, resourceClass, inputMimeType),
                fhirEncoders.of(resourceClass));
    }

    @Nonnull
    public Dataset<Row> encodeBundle(@Nonnull final Dataset<String> stringResources,
                                     @Nonnull final String resourceName,
                                     @Nonnull final String inputMimeType) {

        RuntimeResourceDefinition definition = FhirEncoders.contextFor(fhirVersion)
                .getResourceDefinition(resourceName);
        return encodeBundle(stringResources, definition.getImplementingClass(), inputMimeType).toDF();
    }


    @Nonnull
    public Dataset<Row> pyEncodeBundle(@Nonnull final Dataset<Row> stringResourcesDF,
                                       @Nonnull final String resourceName,
                                       @Nullable final String maybeInputMimeType,
                                       @Nullable final String maybeColumnName) {

        final Dataset<String> stringResources = (nonNull(maybeColumnName) ?
                stringResourcesDF.select(maybeColumnName) : stringResourcesDF).as(Encoders.STRING());
        return encodeBundle(
                stringResources,
                resourceName,
                nonNull(maybeInputMimeType) ? maybeInputMimeType : FhirMimeTypes.FHIR_JSON);
    }
}
