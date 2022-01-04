/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.utilities;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("UnitTest")
public class StringsTest {

  @Test
  public void testParseCsvList() {
    assertEquals(Collections.emptyList(), Strings.parseCsvList("", Integer::parseInt));
    assertEquals(Collections.emptyList(), Strings.parseCsvList(", ,, ", Integer::parseInt));
    assertEquals(Arrays.asList(1, 2, 4), Strings.parseCsvList(", 1, 2, , 4 , ", Integer::parseInt));
  }
}
