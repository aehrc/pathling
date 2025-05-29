package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Stream;

/**
 * Tests for terminology functions.
 */
@Tag("UnitTest")
public class TerminologyFunctionsDslTest extends FhirPathDslTestBase {

  @Autowired
  TerminologyService terminologyService;

  @FhirPathTest
  public Stream<DynamicTest> testDisplayFunction() {
    // Reset mocks and setup terminology service expectations
    SharedMocks.resetAll();
    
    // Create codings for testing
    Coding loincCoding = new Coding("http://loinc.org", "55915-3", 
        "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis");
    Coding snomedCoding = new Coding("http://snomed.info/sct", "63816008", "Left hepatectomy");
    Coding weightCoding = new Coding("http://loinc.org", "29463-7", "Body weight");
    
    // Setup terminology service expectations
    TerminologyServiceHelpers.setupLookup(terminologyService)
        // Setup display values without language parameter
        .withDisplay(loincCoding)
        .withDisplay(snomedCoding)
        .withDisplay(weightCoding)
        
        // Setup display values with language parameter
        .withDisplay(loincCoding, "LC_55915_3 (DE)", "de")
        .withDisplay(snomedCoding, "CD_SNOMED_VER_63816008 (DE)", "de")
        .withDisplay(weightCoding, "LC_29463_7 (DE)", "de")
        .done();

    return builder()
        .withSubject(sb -> sb
            // Empty coding
            .coding("emptyCoding", null)
            // Single codings
            .coding("loinc",
                "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis'")
            .coding("snomed", "http://snomed.info/sct|63816008||'Left hepatectomy'")
            // Array of codings
            .codingArray("multipleCodings",
                "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis'",
                "http://snomed.info/sct|63816008||'Left hepatectomy'",
                "http://loinc.org|29463-7||'Body weight'")
        )
        .group("display() function with no language parameter")
        .testEquals("Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis",
            "loinc.display()",
            "display() returns the display value for a single LOINC coding")
        .testEquals("Left hepatectomy", "snomed.display()",
            "display() returns the display value for a single SNOMED coding")
        .testEmpty("emptyCoding.display()",
            "display() returns empty for an empty coding")
        .testEquals("Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis",
            "multipleCodings[0].display()",
            "display() returns the display value for the first coding in an array")
        .testEquals("Left hepatectomy", "multipleCodings[1].display()",
            "display() returns the display value for the second coding in an array")
        .testEquals("Body weight", "multipleCodings[2].display()",
            "display() returns the display value for the third coding in an array")

        .group("display() function with language parameter")
        .testEquals("LC_55915_3 (DE)", "loinc.display('de')",
            "display() with language parameter returns the localized display value for LOINC")
        .testEquals("CD_SNOMED_VER_63816008 (DE)", "snomed.display('de')",
            "display() with language parameter returns the localized display value for SNOMED")
        .testEmpty("emptyCoding.display('de')",
            "display() with language parameter returns empty for an empty coding")
        .testEquals("LC_55915_3 (DE)", "multipleCodings[0].display('de')",
            "display() with language parameter returns the localized display value for the first coding in an array")
        .testEquals("CD_SNOMED_VER_63816008 (DE)", "multipleCodings[1].display('de')",
            "display() with language parameter returns the localized display value for the second coding in an array")
        .testEquals("LC_29463_7 (DE)", "multipleCodings[2].display('de')",
            "display() with language parameter returns the localized display value for the third coding in an array")
            
        .group("display() function on collections")
        .testEquals(List.of("Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis",  
                        "Left hepatectomy", "Body weight"),
            "multipleCodings.display()", 
            "display() returns display names on a collection of codings")
        .testEquals(List.of("LC_55915_3 (DE)", "CD_SNOMED_VER_63816008 (DE)", "LC_29463_7 (DE)"),
            "multipleCodings.display('de')", 
            "display() returns localized display names on a collection of codings")
        
        .group("display() function error cases")
        .testError("'string'.display()",
            "display() throws an error when called on a non-coding type")
        .testError("loinc.display('en', 'fr')",
            "display() throws an error when called with more than one parameter")
        .build();
  }
}
