/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(
    properties = {
        "pathling.auth.enabled=false",
        "pathling.caching.enabled=false"
    })
public class SecurityDisabledResourcesTest extends SecurityTestForResources {

  @Test
  public void testPassIfResourceWriteWithNoAuth() {
    assertWriteSuccess();
  }

  @Test
  public void testPassIfResourceReadWithNoAuth() {
    assertReadSuccess();
  }
}
