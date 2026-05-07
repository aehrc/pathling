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

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BulkSubmitConfiguration}.
 *
 * @author John Grimes
 */
class BulkSubmitConfigurationTest {

  @Test
  void emptyAllowableSourcesRejectsAllUrls() {
    // Given: a configuration with empty allowableSources.
    final BulkSubmitConfiguration config = new BulkSubmitConfiguration();
    config.setAllowableSources(new ArrayList<>());

    // Then: every URL is rejected, since the allowlist is mandatory.
    assertThat(config.isSourceAllowed("https://example.org/file.ndjson")).isFalse();
    assertThat(config.isSourceAllowed("http://localhost:8080/file.ndjson")).isFalse();
    assertThat(config.isSourceAllowed("s3://bucket/file.ndjson")).isFalse();
  }

  @Test
  void urlMatchingPrefixIsAllowed() {
    // Given: a configuration with a specific prefix.
    final BulkSubmitConfiguration config = new BulkSubmitConfiguration();
    config.setAllowableSources(List.of("https://allowed.example.org/"));

    // Then: a URL starting with that prefix is allowed.
    assertThat(config.isSourceAllowed("https://allowed.example.org/file.ndjson")).isTrue();
    assertThat(config.isSourceAllowed("https://allowed.example.org/path/to/file.ndjson")).isTrue();
  }

  @Test
  void urlNotMatchingAnyPrefixIsRejected() {
    // Given: a configuration with a specific prefix.
    final BulkSubmitConfiguration config = new BulkSubmitConfiguration();
    config.setAllowableSources(List.of("https://allowed.example.org/"));

    // Then: a URL not starting with that prefix is rejected.
    assertThat(config.isSourceAllowed("https://disallowed.example.org/file.ndjson")).isFalse();
    assertThat(config.isSourceAllowed("http://allowed.example.org/file.ndjson")).isFalse();
  }

  @Test
  void prefixMatchingIsLiteralNotDomainSuffix() {
    // Given: a configuration with a prefix that is a domain.
    final BulkSubmitConfiguration config = new BulkSubmitConfiguration();
    config.setAllowableSources(List.of("https://example.org/"));

    // Then: a URL with additional domain components after the prefix is rejected.
    assertThat(config.isSourceAllowed("https://example.org.evil/file.ndjson")).isFalse();
    assertThat(config.isSourceAllowed("https://example.org.attacker.com/file.ndjson")).isFalse();

    // And: legitimate subpaths are still allowed.
    assertThat(config.isSourceAllowed("https://example.org/subdir/file.ndjson")).isTrue();
  }

  @Test
  void multiplePrefixesAreChecked() {
    // Given: a configuration with multiple prefixes.
    final BulkSubmitConfiguration config = new BulkSubmitConfiguration();
    config.setAllowableSources(
        List.of("https://allowed1.example.org/", "https://allowed2.example.org/"));

    // Then: URLs matching either prefix are allowed.
    assertThat(config.isSourceAllowed("https://allowed1.example.org/file.ndjson")).isTrue();
    assertThat(config.isSourceAllowed("https://allowed2.example.org/file.ndjson")).isTrue();

    // And: URLs not matching any prefix are rejected.
    assertThat(config.isSourceAllowed("https://disallowed.example.org/file.ndjson")).isFalse();
  }
}
