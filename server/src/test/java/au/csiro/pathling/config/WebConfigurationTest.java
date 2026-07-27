/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.DelegatingWebMvcConfiguration;

/**
 * Tests the caching behaviour of the admin UI resource handlers registered by {@link
 * WebConfiguration}, against the contract recorded in the feature's http-caching contract.
 *
 * <p>The documents served here come from the fixtures under {@code
 * src/test/resources/static/admin/}, which shadow any real UI build on the test classpath. The real
 * UI is built at the {@code prepare-package} phase, after unit tests run, so these tests must not
 * depend on it being present.
 *
 * <p>{@link DelegatingWebMvcConfiguration} is registered directly rather than through a local
 * configuration class carrying {@code @EnableWebMvc}. It is what {@code @EnableWebMvc} imports, and
 * registering it here avoids putting another {@code @Configuration} class on the test classpath,
 * which {@code TestDataImporter} would pick up in its component scan and then fail to instantiate
 * outside a servlet environment.
 *
 * @author John Grimes
 */
@ExtendWith(SpringExtension.class)
@WebAppConfiguration
@ContextConfiguration(classes = {DelegatingWebMvcConfiguration.class, WebConfiguration.class})
class WebConfigurationTest {

  /** The validator that every published container image reported, from the issue. */
  private static final String STUCK_CLIENT_VALIDATOR = "Thu, 01 Jan 1970 00:00:01 GMT";

  /** A validator later than any conceivable build, to show the comparison is not made at all. */
  private static final String FAR_FUTURE_VALIDATOR = "Wed, 01 Jan 2099 00:00:01 GMT";

  private static final String ENTRY_DOCUMENT_CACHE_CONTROL = "no-store, must-revalidate";

  private static final String HASHED_ASSET_CACHE_CONTROL = "max-age=31536000, public, immutable";

  @Autowired @Nonnull WebApplicationContext webApplicationContext;

  @Nonnull MockMvc mockMvc;

  @BeforeEach
  void setUp() {
    mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
  }

  // The entry document names the hashed assets of one specific build, so a stored copy pins the
  // whole UI to the version it was built from.
  @Test
  void entryDocumentIsNotStorable() throws Exception {
    mockMvc
        .perform(get("/admin/"))
        .andExpect(status().isOk())
        .andExpect(header().string("Cache-Control", ENTRY_DOCUMENT_CACHE_CONTROL));
  }

  // Without a validator there is nothing a conditional request can match, so the handler cannot
  // answer one with a not-modified response.
  @Test
  void entryDocumentHasNoValidator() throws Exception {
    mockMvc
        .perform(get("/admin/"))
        .andExpect(status().isOk())
        .andExpect(header().doesNotExist("Last-Modified"))
        .andExpect(header().doesNotExist("ETag"));
  }

  // This is the case that recovers a browser already stuck on an old bundle: it revalidates with
  // the validator it was given, and must be answered with the current document.
  @Test
  void entryDocumentIgnoresConditionalRequest() throws Exception {
    assertCurrentDocumentReturned("/admin/", STUCK_CLIENT_VALIDATOR);
    assertCurrentDocumentReturned("/admin/", FAR_FUTURE_VALIDATOR);
  }

  // A client-side route must not become an independent way to pin an old bundle, so the fallback
  // carries the same headers as the entry document itself.
  @Test
  void spaFallbackMatchesEntryDocument() throws Exception {
    final String entryDocument = assertEntryDocumentContract("/admin/");

    assertThat(assertEntryDocumentContract("/admin/jobs")).isEqualTo(entryDocument);
    assertThat(assertEntryDocumentContract("/admin/does-not-exist")).isEqualTo(entryDocument);
    assertCurrentDocumentReturned("/admin/jobs", STUCK_CLIENT_VALIDATOR);
  }

  // The explicit path resolves to a real file rather than to the fallback, so it is asserted
  // separately to show that it is not a hole in the contract.
  @Test
  void indexHtmlPathMatchesEntryDocument() throws Exception {
    final String entryDocument = assertEntryDocumentContract("/admin/");

    assertThat(assertEntryDocumentContract("/admin/index.html")).isEqualTo(entryDocument);
    assertCurrentDocumentReturned("/admin/index.html", STUCK_CLIENT_VALIDATOR);
  }

  // Every filename under the assets directory carries a content hash, so its body can never change
  // and revalidation is pure cost, even on a user-initiated reload.
  @Test
  void hashedAssetsAreImmutable() throws Exception {
    mockMvc
        .perform(get("/admin/assets/index-test.js"))
        .andExpect(status().isOk())
        .andExpect(header().string("Cache-Control", HASHED_ASSET_CACHE_CONTROL));
  }

  /**
   * Asserts that the given path satisfies the entry document contract, and returns the body it
   * served so that callers can compare bodies across paths.
   *
   * @param path the path to request
   * @return the response body
   * @throws Exception if the request cannot be performed
   */
  @Nonnull
  private String assertEntryDocumentContract(@Nonnull final String path) throws Exception {
    final MvcResult result =
        mockMvc
            .perform(get(path))
            .andExpect(status().isOk())
            .andExpect(header().string("Cache-Control", ENTRY_DOCUMENT_CACHE_CONTROL))
            .andExpect(header().doesNotExist("Last-Modified"))
            .andExpect(header().doesNotExist("ETag"))
            .andReturn();
    final String body = result.getResponse().getContentAsString();
    assertThat(body).isNotEmpty();
    return body;
  }

  /**
   * Asserts that a conditional request for the given path is answered with the current document
   * rather than with a not-modified response.
   *
   * @param path the path to request
   * @param validator the value to send in the {@code If-Modified-Since} header
   * @throws Exception if the request cannot be performed
   */
  private void assertCurrentDocumentReturned(
      @Nonnull final String path, @Nonnull final String validator) throws Exception {
    final MvcResult result =
        mockMvc
            .perform(get(path).header("If-Modified-Since", validator))
            .andExpect(status().isOk())
            .andReturn();
    assertThat(result.getResponse().getContentAsString()).isNotEmpty();
  }
}
