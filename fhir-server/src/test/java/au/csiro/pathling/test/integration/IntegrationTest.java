/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import org.apache.catalina.webresources.TomcatURLStreamHandlerFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;

/**
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
public abstract class IntegrationTest {

  public static final String INDIVIDUAL_TEST_WAREHOUSE = "src/test/resources/test-data/tests";

  @BeforeAll
  static void beforeAll() {
    // See: https://github.com/spring-projects/spring-boot/issues/21535#issuecomment-634088332
    TomcatURLStreamHandlerFactory.disable();
  }

}
