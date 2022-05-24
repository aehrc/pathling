package au.csiro.pathling.encoders.utils;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.hl7.fhir.instance.model.api.IBaseResource;

import java.io.FileReader;
import java.io.Reader;

public class GenerateTurtleBundles {
    public static void main(final String[] args) throws Exception {


        FhirContext fhirContext = FhirContext.forR4();

        final IParser jsonPaser = fhirContext.newJsonParser();
        final IParser rdfParser = fhirContext.newRDFParser();

        try (Reader reader = new FileReader("src/test/resources/data/bundles/R4/json/Bennett146_Swaniawski813_704c9750-f6e6-473b-ee83-fbd48e07fe3f.json")) {
            IBaseResource bundle = jsonPaser.parseResource(reader);
        }
    }
}
