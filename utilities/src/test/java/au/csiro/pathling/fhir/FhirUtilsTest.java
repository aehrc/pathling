package au.csiro.pathling.fhir;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hl7.fhir.exceptions.FHIRException;
import org.junit.jupiter.api.Test;

class FhirUtilsTest {

  @Test
  void nullThrowsError() {
    assertThrows(FHIRException.class, () -> FhirUtils.getResourceType(null));
  }

  @Test
  void emptyStringThrowsError() {
    assertThrows(FHIRException.class, () -> FhirUtils.getResourceType(""));
  }
  
}
