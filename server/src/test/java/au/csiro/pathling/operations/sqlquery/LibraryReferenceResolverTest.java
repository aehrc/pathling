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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests for {@link LibraryReferenceResolver#tryResolveSqlViewLibrary}, covering canonical matching
 * against {@code Library.url} and the selection among several matching versions.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
class LibraryReferenceResolverTest {

  private static final String FOO_URL = "https://example.org/Library/foo";

  @Autowired private SparkSession spark;
  @Autowired private FhirEncoders fhirEncoders;

  private DataSource dataSource;
  private LibraryReferenceResolver resolver;

  @BeforeEach
  void setUp() {
    dataSource = mock(DataSource.class);
    resolver = new LibraryReferenceResolver(dataSource, fhirEncoders, authDisabledConfig());
  }

  @Test
  void resolvesByCanonicalUrl() {
    when(dataSource.read("Library"))
        .thenReturn(libraryDataset(newLibrary("a", FOO_URL, "1.0", PublicationStatus.ACTIVE)));

    assertThat(resolvedId(FOO_URL)).contains("Library/a");
  }

  @Test
  void resolvesByCanonicalUrlWithVersion() {
    when(dataSource.read("Library"))
        .thenReturn(
            libraryDataset(
                newLibrary("a", FOO_URL, "1.0", PublicationStatus.ACTIVE),
                newLibrary("b", FOO_URL, "2.0", PublicationStatus.ACTIVE)));

    assertThat(resolvedId(FOO_URL + "|2.0")).contains("Library/b");
  }

  @Test
  void prefersActiveOverDraftWhenNoVersionSupplied() {
    when(dataSource.read("Library"))
        .thenReturn(
            libraryDataset(
                newLibrary("a", FOO_URL, "1.0", PublicationStatus.DRAFT),
                newLibrary("b", FOO_URL, "1.0", PublicationStatus.ACTIVE)));

    assertThat(resolvedId(FOO_URL)).contains("Library/b");
  }

  @Test
  void picksLatestActiveVersionWhenNoneSupplied() {
    when(dataSource.read("Library"))
        .thenReturn(
            libraryDataset(
                newLibrary("a", FOO_URL, "1.0", PublicationStatus.ACTIVE),
                newLibrary("b", FOO_URL, "2.0", PublicationStatus.ACTIVE)));

    assertThat(resolvedId(FOO_URL)).contains("Library/b");
  }

  @Test
  void resolvesUrnCanonical() {
    when(dataSource.read("Library"))
        .thenReturn(
            libraryDataset(newLibrary("a", "urn:uuid:abc-123", "1.0", PublicationStatus.ACTIVE)));

    assertThat(resolvedId("urn:uuid:abc-123")).contains("Library/a");
  }

  @Test
  void returnsEmptyWhenCanonicalDoesNotMatch() {
    // A non-match is not an error, because the canonical may name a ViewDefinition instead.
    when(dataSource.read("Library")).thenReturn(libraryDataset());

    assertThat(resolvedId("https://example.org/Library/missing")).isEmpty();
  }

  @Test
  void returnsEmptyWhenVersionDoesNotMatch() {
    when(dataSource.read("Library"))
        .thenReturn(libraryDataset(newLibrary("a", FOO_URL, "1.0", PublicationStatus.ACTIVE)));

    assertThat(resolvedId(FOO_URL + "|99.0")).isEmpty();
  }

  @Test
  void returnsEmptyWhenTheServerHoldsNoLibraries() {
    // The data source signals an absent resource type with an IllegalArgumentException.
    when(dataSource.read("Library"))
        .thenThrow(new IllegalArgumentException("No data found for resource type Library"));

    assertThat(resolvedId(FOO_URL)).isEmpty();
  }

  /** Resolves the canonical and returns the id of the selected Library, if any. */
  private Optional<String> resolvedId(final String canonical) {
    return resolver.tryResolveSqlViewLibrary(canonical).map(Library::getId);
  }

  private Dataset<Row> libraryDataset(final Library... libraries) {
    return spark.createDataset(List.of(libraries), fhirEncoders.of("Library")).toDF();
  }

  /** Builds a server configuration with authorisation disabled, so no metadata READ is enforced. */
  private static ServerConfiguration authDisabledConfig() {
    final ServerConfiguration config = new ServerConfiguration();
    final AuthorizationConfiguration auth = new AuthorizationConfiguration();
    auth.setEnabled(false);
    config.setAuth(auth);
    return config;
  }

  private static Library newLibrary(
      final String id, final String url, final String version, final PublicationStatus status) {
    final Library library = new Library();
    library.setId("Library/" + id);
    library.setUrl(url);
    library.setVersion(version);
    library.setStatus(status);
    return library;
  }
}
