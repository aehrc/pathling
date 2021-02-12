package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("UnitTest")
public class TerminologyUtilsTest {

  @Test
  public void testParseCsvList() {
    assertEquals(Arrays.asList(1, 2, 4), TerminologyUtils.parseCsvList("1, 2, , 4 ", Integer::parseInt));
  }
}
