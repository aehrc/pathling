/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import org.junit.jupiter.api.Test;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.TestPropertySource;

/**
 * @see <a href="https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html">Spring
 * Security - Testing</a>
 * @see <a href="https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property">In
 * Spring Boot Test, how do I map a temporary folder to a configuration property?</a>
 */
@TestPropertySource(locations = {"classpath:/configuration/authorization.properties"},
    properties = {
        "pathling.caching.enabled=false"
    })
public class SecurityEnabledTestForOperations extends SecurityTestForOperations {

  @Test
  @WithMockUser(username = "admin")
  public void testForbiddenIfImportWithoutAuthority() {
    assertThrowsAccessDenied(this::assertImportSuccess, "pathling:import"
    );
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:import"})
  public void testPassIfImportWithAuthority() {
    assertImportSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  public void testForbiddenIfAggregateWithoutAuthority() {
    assertThrowsAccessDenied(this::assertAggregateSuccess, "pathling:aggregate"
    );
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:aggregate"})
  public void testPassIfAggregateWithAuthority() {
    assertAggregateSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  public void testForbiddenIfSearchWithoutAuthority() {
    assertThrowsAccessDenied(this::assertSearchSuccess, "pathling:search"
    );
    assertThrowsAccessDenied(this::assertSearchWithFilterSuccess, "pathling:search"
    );
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:search"})
  public void testPassIfSearchWithAuthority() {
    assertSearchSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  public void testForbiddenIfCachingSearchWithoutAuthority() {
    // TODO: this become somewhat messy because of the current caching implementation.
    //  Perhaps we could use AOP here as well to simplify?
    assertThrowsAccessDenied(this::assertCachingSearchSuccess, "pathling:search");
    assertThrowsAccessDenied(this::assertCachingSearchWithFilterSuccess, "pathling:search");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:search"})
  public void testPassIfCachingSearchWithAuthority() {
    assertCachingSearchSuccess();
  }
}
