package au.csiro.pathling.api;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;

import javax.annotation.Nonnull;

public class FhirParserFactory {

    @Nonnull
    private final FhirContext fhirContext;

    private FhirParserFactory(@Nonnull final FhirContext fhirContext) {
        this.fhirContext = fhirContext;
    }

    @Nonnull
    public IParser createParser(@Nonnull final String mimeType) {
        switch (mimeType) {
            case "application/fhir+json":
                return fhirContext.newJsonParser();
            case "application/fhir+xml":
                return fhirContext.newXmlParser();
            case "application/fhir+turtle":
                return fhirContext.newRDFParser();
            default:
                throw new IllegalArgumentException("Cannot create FHIR parser for mime type: " + mimeType);
        }
    }

    @Nonnull
    public static FhirParserFactory forContext(@Nonnull final FhirContext fhirContext) {
        return new FhirParserFactory(fhirContext);
    }

    @Nonnull
    public static FhirParserFactory forVersion(@Nonnull final FhirVersionEnum fhirVersion) {
        return new FhirParserFactory(FhirEncoders.contextFor(fhirVersion));
    }
}
