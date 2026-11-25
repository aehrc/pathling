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

import au.csiro.pathling.operations.bulksubmit.SubmitterIdentifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

/**
 * Tests for {@link BulkSubmitConfiguration}.
 *
 * @author John Grimes
 */
class BulkSubmitConfigurationTest {

  private static final String SYSTEM = "http://example.org/submitters";
  private static final SubmitterIdentifier SUBMITTER = new SubmitterIdentifier(SYSTEM, "sub-1");

  // -- Shared allowableSources, applied to a submitter without its own list --

  @Test
  void emptyAllowableSourcesRejectsAllUrls() {
    // Given: a configuration with empty allowableSources.
    final BulkSubmitConfiguration config = configWith(new ArrayList<>(), submitter(List.of()));

    // Then: every URL is rejected, since the allowlist is mandatory.
    assertThat(config.isSourceAllowed(SUBMITTER, "https://example.org/file.ndjson", false))
        .isFalse();
    assertThat(config.isSourceAllowed(SUBMITTER, "http://localhost:8080/file.ndjson", false))
        .isFalse();
    assertThat(config.isSourceAllowed(SUBMITTER, "s3://bucket/file.ndjson", false)).isFalse();
  }

  @Test
  void urlMatchingPrefixIsAllowed() {
    // Given: a configuration with a specific prefix.
    final BulkSubmitConfiguration config =
        configWith(List.of("https://allowed.example.org/"), submitter(List.of()));

    // Then: a URL starting with that prefix is allowed.
    assertThat(config.isSourceAllowed(SUBMITTER, "https://allowed.example.org/file.ndjson", false))
        .isTrue();
    assertThat(
            config.isSourceAllowed(
                SUBMITTER, "https://allowed.example.org/path/to/file.ndjson", false))
        .isTrue();
  }

  @Test
  void urlNotMatchingAnyPrefixIsRejected() {
    // Given: a configuration with a specific prefix.
    final BulkSubmitConfiguration config =
        configWith(List.of("https://allowed.example.org/"), submitter(List.of()));

    // Then: a URL not starting with that prefix is rejected.
    assertThat(
            config.isSourceAllowed(SUBMITTER, "https://disallowed.example.org/file.ndjson", false))
        .isFalse();
    assertThat(config.isSourceAllowed(SUBMITTER, "http://allowed.example.org/file.ndjson", false))
        .isFalse();
  }

  @Test
  void prefixMatchingIsLiteralNotDomainSuffix() {
    // Given: a configuration with a prefix that is a domain.
    final BulkSubmitConfiguration config =
        configWith(List.of("https://example.org/"), submitter(List.of()));

    // Then: a URL with additional domain components after the prefix is rejected.
    assertThat(config.isSourceAllowed(SUBMITTER, "https://example.org.evil/file.ndjson", false))
        .isFalse();
    assertThat(
            config.isSourceAllowed(
                SUBMITTER, "https://example.org.attacker.com/file.ndjson", false))
        .isFalse();

    // And: legitimate subpaths are still allowed.
    assertThat(config.isSourceAllowed(SUBMITTER, "https://example.org/subdir/file.ndjson", false))
        .isTrue();
  }

  @Test
  void multiplePrefixesAreChecked() {
    // Given: a configuration with multiple prefixes.
    final BulkSubmitConfiguration config =
        configWith(
            List.of("https://allowed1.example.org/", "https://allowed2.example.org/"),
            submitter(List.of()));

    // Then: URLs matching either prefix are allowed.
    assertThat(config.isSourceAllowed(SUBMITTER, "https://allowed1.example.org/file.ndjson", false))
        .isTrue();
    assertThat(config.isSourceAllowed(SUBMITTER, "https://allowed2.example.org/file.ndjson", false))
        .isTrue();

    // And: URLs not matching any prefix are rejected.
    assertThat(
            config.isSourceAllowed(SUBMITTER, "https://disallowed.example.org/file.ndjson", false))
        .isFalse();
  }

  // -- Submitter-level allowableSources --

  @Test
  void submitterSourcesReplaceSharedSources() {
    // Given: shared sources covering two hosts, and a submitter whose own list covers only one.
    final BulkSubmitConfiguration config =
        configWith(
            List.of("https://a.example.org/", "https://b.example.org/"),
            submitter(List.of("https://a.example.org/")));

    // Then: a URL on the submitter's own host is allowed.
    assertThat(config.isSourceAllowed(SUBMITTER, "https://a.example.org/m.json", false)).isTrue();

    // And: a URL on a host that only the shared list covers is rejected for this submitter.
    assertThat(config.isSourceAllowed(SUBMITTER, "https://b.example.org/m.json", false)).isFalse();
  }

  @Test
  void submitterSourcesAreNotSharedWithOtherSubmitters() {
    // Given: two submitters, each with its own sources, and no shared sources.
    final SubmitterIdentifier other = new SubmitterIdentifier(SYSTEM, "sub-2");
    final BulkSubmitConfiguration config =
        configWith(
            List.of(),
            submitter(List.of("https://a.example.org/")),
            new SubmitterConfiguration(
                SYSTEM,
                "sub-2",
                null,
                null,
                null,
                null,
                null,
                null,
                List.of("https://b.example.org/")));

    // Then: each submitter is allowed only its own host.
    assertThat(config.isSourceAllowed(SUBMITTER, "https://b.example.org/m.json", false)).isFalse();
    assertThat(config.isSourceAllowed(other, "https://a.example.org/m.json", false)).isFalse();
    assertThat(config.isSourceAllowed(other, "https://b.example.org/m.json", false)).isTrue();
  }

  @Test
  void submitterWithCredentialsAndNoOwnSourcesRejectsAllUrls() {
    // Given: a submitter with OAuth credentials but no allowableSources of its own.
    final BulkSubmitConfiguration config =
        configWith(
            List.of("https://a.example.org/"),
            new SubmitterConfiguration(
                SYSTEM, "sub-1", "client-id", "client-secret", null, null, null, null, List.of()));

    // Then: the shared list is not used for it, so every URL is rejected.
    assertThat(config.isSourceAllowed(SUBMITTER, "https://a.example.org/m.json", false)).isFalse();
  }

  @Test
  void submitterWithCredentialsAndOwnSourcesAllowsOwnSources() {
    // Given: a submitter with OAuth credentials and its own allowableSources.
    final BulkSubmitConfiguration config =
        configWith(
            List.of("https://b.example.org/"),
            new SubmitterConfiguration(
                SYSTEM,
                "sub-1",
                "client-id",
                "client-secret",
                null,
                null,
                null,
                null,
                List.of("https://a.example.org/")));

    // Then: only its own sources are allowed.
    assertThat(config.isSourceAllowed(SUBMITTER, "https://a.example.org/m.json", false)).isTrue();
    assertThat(config.isSourceAllowed(SUBMITTER, "https://b.example.org/m.json", false)).isFalse();
  }

  @Test
  void unconfiguredSubmitterRejectsAllUrls() {
    // Given: shared sources, but the submitter is not in the allowed submitters list.
    final BulkSubmitConfiguration config = configWith(List.of("https://a.example.org/"));

    // Then: every URL is rejected for it.
    assertThat(config.isSourceAllowed(SUBMITTER, "https://a.example.org/m.json", false)).isFalse();
  }

  // -- Property binding --

  @Test
  void bindsSubmitterAllowableSources() {
    // Given: a submitter configured with its own allowableSources through properties.
    final BulkSubmitConfiguration config =
        bind(
            Map.of(
                "pathling.bulk-submit.allowed-submitters[0].system", SYSTEM,
                "pathling.bulk-submit.allowed-submitters[0].value", "sub-1",
                "pathling.bulk-submit.allowed-submitters[0].allowable-sources[0]",
                    "https://a.example.org/"));

    // Then: the list is bound onto the submitter.
    assertThat(config.getAllowedSubmitters())
        .singleElement()
        .extracting(SubmitterConfiguration::allowableSources)
        .isEqualTo(List.of("https://a.example.org/"));
  }

  @Test
  void bindsAbsentSubmitterAllowableSourcesAsEmpty() {
    // Given: a submitter configured without allowableSources.
    final BulkSubmitConfiguration config =
        bind(
            Map.of(
                "pathling.bulk-submit.allowed-submitters[0].system",
                SYSTEM,
                "pathling.bulk-submit.allowed-submitters[0].value",
                "sub-1"));

    // Then: the submitter has an empty list rather than null.
    assertThat(config.getAllowedSubmitters())
        .singleElement()
        .extracting(SubmitterConfiguration::allowableSources)
        .isEqualTo(List.of());
  }

  // -- Helpers --

  private static SubmitterConfiguration submitter(final List<String> allowableSources) {
    return new SubmitterConfiguration(
        SYSTEM, "sub-1", null, null, null, null, null, null, allowableSources);
  }

  private static BulkSubmitConfiguration configWith(
      final List<String> sharedSources, final SubmitterConfiguration... submitters) {
    final BulkSubmitConfiguration config = new BulkSubmitConfiguration();
    config.setAllowableSources(sharedSources);
    config.setAllowedSubmitters(List.of(submitters));
    return config;
  }

  private static BulkSubmitConfiguration bind(final Map<String, String> properties) {
    return new Binder(new MapConfigurationPropertySource(properties))
        .bind("pathling.bulk-submit", BulkSubmitConfiguration.class)
        .get();
  }
}
