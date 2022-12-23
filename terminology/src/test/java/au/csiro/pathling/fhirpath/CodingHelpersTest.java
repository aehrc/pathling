package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.Test;

class CodingHelpersTest {

  @Nonnull
  static Coding newCoding(@Nullable final String system, @Nullable final String code,
      @Nullable final String version) {
    return new Coding(system, code, null).setVersion(version);
  }

  @Nonnull
  static Coding newCoding(@Nullable final String system, @Nullable final String code) {
    return newCoding(system, code, null);
  }


  static void assertCodingEq(@Nullable final Coding left, @Nullable final Coding right) {
    assertTrue(CodingHelpers.codingEquals(left, right));
    assertTrue(CodingHelpers.codingEquals(right, left));
  }


  static void assertCodingNotEq(@Nullable final Coding left, @Nullable final Coding right) {
    assertFalse(CodingHelpers.codingEquals(left, right));
    assertFalse(CodingHelpers.codingEquals(right, left));
  }

  @Test
  void codingEquals() {
    assertCodingEq(null, null);
    assertCodingEq(newCoding("s1", "c1", "v1"), newCoding("s1", "c1", "v1"));
    assertCodingEq(newCoding("s1", "c1"), newCoding("s1", "c1", "v1"));
    assertCodingEq(newCoding("s1", "c1"), newCoding("s1", "c1"));
    assertCodingEq(newCoding("s1", null), newCoding("s1", null));
    assertCodingEq(newCoding(null, null), newCoding(null, null));
  }

  @Test
  void codingNotEquals() {
    assertCodingNotEq(newCoding("s1", "c1", "v1"), newCoding("s1", "c1", "v2"));
    assertCodingNotEq(newCoding("s1", "c1", "v1"), newCoding("s1", "c2"));
    assertCodingNotEq(newCoding("s1", "c1", "v1"), newCoding("s1", null));
    assertCodingNotEq(newCoding("s1", "c1"), newCoding("s2", "c1"));
    assertCodingNotEq(newCoding("s1", "c1"), newCoding(null, "c1"));
  }
}
