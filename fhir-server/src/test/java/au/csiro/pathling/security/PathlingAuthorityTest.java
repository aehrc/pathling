package au.csiro.pathling.security;


import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.security.PathlingAuthority.AccessType;
import java.util.Arrays;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

@Tag("UnitTest")
public class PathlingAuthorityTest {

  @Nonnull
  private static GrantedAuthority auth(@Nonnull final String authority) {
    return new SimpleGrantedAuthority(authority);
  }

  @Test
  public void testResourceAccessSubsumedBy() {

    final PathlingAuthority patientRead = PathlingAuthority
        .resourceAccess(ResourceType.PATIENT, AccessType.READ);

    final PathlingAuthority conditionWrite = PathlingAuthority
        .resourceAccess(ResourceType.CONDITION, AccessType.WRITE);

    // positive cases
    assertTrue(patientRead.subsumedBy(auth("user/Patient.read")));
    assertTrue(patientRead.subsumedBy(auth("user/*.read")));

    assertTrue(conditionWrite.subsumedBy(auth("user/Condition.write")));
    assertTrue(conditionWrite.subsumedBy(auth("user/*.write")));

    // negative cases
    assertFalse(patientRead.subsumedBy(auth("user/*.write")));
    assertFalse(conditionWrite.subsumedBy(auth("user/*.read")));

    // Exotic cases
    // TODO: In the current implementation '*' does NOT match separators (/.:) but
    // only word characters. That can be easily changed though.
    assertTrue(patientRead.subsumedBy(auth("*/*.*")));
    assertFalse(patientRead.subsumedBy(auth("*")));
  }

  @Test
  public void testOperationAccessSubsumedBy() {

    final PathlingAuthority searchAccess = PathlingAuthority.operationAccess("search");

    // positive cases
    assertTrue(searchAccess.subsumedBy(auth("operation:search")));
    assertTrue(searchAccess.subsumedBy(auth("operation:*")));

    // negative cases
    assertFalse(searchAccess.subsumedBy(auth("operation:import")));

    // Exotic cases
    // TODO: In the current implementation '*' does NOT match separators (/.:) but
    // only word characters. That can be easily changed though.
    assertTrue(searchAccess.subsumedBy(auth("*:*")));
    assertFalse(searchAccess.subsumedBy(auth("*")));
  }

  @Test
  public void testSubsumedByAny() {

    final PathlingAuthority operationSearch = PathlingAuthority.fromAuthority("operation:search");
    // Negative cases
    assertFalse(operationSearch.subsumedByAny(Collections.emptyList()));
    assertFalse(
        operationSearch.subsumedByAny(Arrays.asList(auth("operation:xxx"), auth("operation:yyy"))));

    assertTrue(
        operationSearch.subsumedByAny(Arrays.asList(auth("operation:xxx"), auth("operation:*"))));
  }
}
