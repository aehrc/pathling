package au.csiro.pathling.api;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.hl7.fhir.instance.model.api.IBaseResource;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static au.csiro.pathling.api.FhirMimeTypes.*;

abstract class EncodeMapPartitionsFunc<T extends IBaseResource> implements MapPartitionsFunction<String, T> {

    private static final long serialVersionUID = -189338116652852324L;
    protected FhirVersionEnum fhirVersion;
    protected final String inputMimeType;
    protected final Class<T> resourceClass;

    protected EncodeMapPartitionsFunc(FhirVersionEnum fhirVersion, final String inputMimeType,
                                      Class<T> resourceClass) {
        this.fhirVersion = fhirVersion;
        this.inputMimeType = inputMimeType;
        this.resourceClass = resourceClass;
    }

    /**
     * Converts the parsed input resouces to the final stream of requested resources.
     * May include filtering, extracting from bundles etc.
     *
     * @param resources the stream of all input resources
     * @return the stream of desired resources
     */
    @Nonnull
    protected abstract Stream<IBaseResource> processResources(@Nonnull final Stream<IBaseResource> resources);

    @Override
    @Nonnull
    public Iterator<T> call(@Nonnull final Iterator<String> iterator) {
        final IParser parser = createParser(inputMimeType);

        final Iterable<String> iterable = () -> iterator;
        final Stream<IBaseResource> parsedResources = StreamSupport.stream(iterable.spliterator(), false)
                .map(parser::parseResource);
        //noinspection unchecked
        return (Iterator<T>) processResources(parsedResources).iterator();
    }

    @Nonnull
    protected IParser createParser(@Nonnull final String mimeType) {
        final FhirContext fhirContext = FhirEncoders.contextFor(fhirVersion);
        switch (mimeType) {
            case FHIR_JSON:
                return fhirContext.newJsonParser();
            case FHIR_XML:
                return fhirContext.newXmlParser();
            case FHIR_TURTLE:
                return fhirContext.newRDFParser();
            default:
                throw new IllegalArgumentException("Cannot create FHIR parser for mime type: " + mimeType);
        }
    }
}