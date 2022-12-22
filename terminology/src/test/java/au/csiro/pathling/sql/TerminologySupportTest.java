package au.csiro.pathling.sql;

import au.csiro.pathling.errors.InvalidUserInputError;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.INEXACT;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.RELATEDTO;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.WIDER;
import static org.junit.jupiter.api.Assertions.*;

class TerminologySupportTest {

  @Test
  void convertsNullAndCsvCodesEnumSet() {
    assertNull(TerminologySupport.parseCsvEquivalences(null));
    assertEquals(Collections.emptySet(),
        TerminologySupport.parseCsvEquivalences(""));
    assertEquals(Collections.emptySet(),
        TerminologySupport.parseCsvEquivalences("  "));
  }

  @Test
  void parseValidCodesToEnumSetIgnoringBlanks() {
    assertEquals(ImmutableSet.of(WIDER, INEXACT),
        TerminologySupport.parseCsvEquivalences("wider, inexact, , wider "));
  }

  @Test
  void throwInvalidUserInputWhenParsingInvalidCodes() {
    final InvalidUserInputError ex = assertThrows(InvalidUserInputError.class,
        () -> TerminologySupport.parseCsvEquivalences("wider, invalid"));
    assertEquals("Unknown ConceptMapEquivalence code 'invalid'", ex.getMessage());
  }


  @Test
  void convertsNullAndEmptyCodesToEnumSet() {
    assertNull(TerminologySupport.equivalenceCodesToEnum(null));
    assertEquals(Collections.emptySet(),
        TerminologySupport.equivalenceCodesToEnum(Collections.emptyList()));
  }


  @Test
  void convertsValidCodesToEnumSet() {
    assertEquals(ImmutableSet.of(EQUIVALENT, RELATEDTO),
        TerminologySupport.equivalenceCodesToEnum(
            List.of("equivalent", "relatedto", "equivalent")));
  }

  @Test
  void throwInvalidUserInputWhenConvertingInvalidCode() {
    final InvalidUserInputError ex = assertThrows(InvalidUserInputError.class,
        () -> TerminologySupport.equivalenceCodesToEnum(List.of("invalid")));

    assertEquals("Unknown ConceptMapEquivalence code 'invalid'", ex.getMessage());
  }
}
