package au.csiro.pathling.export.ws;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AssociatedDataTest {

  @Test
  void testCustomCode() {
    assertEquals("_custom", AssociatedData.custom("custom").toString());
  }

  @Test
  void testFromCodeForValidCodes() {
    assertEquals(AssociatedData.custom("customXXX"), AssociatedData.fromCode("_customXXX"));
    assertEquals(AssociatedData.LATEST_PROVENANCE_RESOURCES,
        AssociatedData.fromCode("LatestProvenanceResources"));
    assertEquals(AssociatedData.RELEVANT_PROVENANCE_RESOURCES,
        AssociatedData.fromCode("RelevantProvenanceResources"));
  }

  @Test
  void testThrowIllegalArguemntForInvalidCode () {
    final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AssociatedData.fromCode("invalid"));
    assertEquals("Unknown associated data code: invalid", ex.getMessage());
  }
}
