package au.csiro.pathling.test.dsl;

import static org.mockito.Mockito.when;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.helpers.TerminologyHelpers;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Sets up the terminology service mock for display function tests.
 */
public class DisplayFunctionDslTestSetup implements BeforeAllCallback {

  @Override
  public void beforeAll(ExtensionContext context) {
    final ApplicationContext applicationContext = SpringExtension.getApplicationContext(context);
    final TerminologyService terminologyService = applicationContext.getBean(TerminologyService.class);
    
    // Setup the terminology service mock for display function tests
    setupDisplayMocks(terminologyService);
  }
  
  private void setupDisplayMocks(@Nonnull final TerminologyService terminologyService) {
    // Setup mocks for display function with no language parameter
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withDisplay(TerminologyHelpers.LC_55915_3)
        .withDisplay(TerminologyHelpers.CD_SNOMED_VER_63816008)
        .withDisplay(TerminologyHelpers.LC_29463_7)
        
        // Setup mocks for display function with language parameter
        .withDisplay(TerminologyHelpers.LC_55915_3, "LC_55915_3 (DE)", "de")
        .withDisplay(TerminologyHelpers.CD_SNOMED_VER_63816008, "CD_SNOMED_VER_63816008 (DE)", "de")
        .withDisplay(TerminologyHelpers.LC_29463_7, "LC_29463_7 (DE)", "de")
        
        // Setup mocks for multiple codings with German language
        .withDisplay(TerminologyHelpers.LC_55915_3, "Beta 2 Globulin (DE)", "de")
        .withDisplay(TerminologyHelpers.CD_SNOMED_VER_63816008, "Linke Hepatektomie (DE)", "de")
        .withDisplay(TerminologyHelpers.LC_29463_7, "Körpergewicht (DE)", "de")
        .done();
  }
}
