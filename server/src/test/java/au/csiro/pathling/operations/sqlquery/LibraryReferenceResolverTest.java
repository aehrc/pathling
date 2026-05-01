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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.read.ReadExecutor;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * Tests for {@link LibraryReferenceResolver} covering both the relative-literal and canonical
 * reference resolution paths.
 */
@TestInstance(Lifecycle.PER_CLASS)
class LibraryReferenceResolverTest {

  // ---------------------------------------------------------------------------
  // Relative literal references — mocked ReadExecutor only.
  // ---------------------------------------------------------------------------

  @Nested
  class RelativeReferences {

    private ReadExecutor readExecutor;
    private LibraryReferenceResolver resolver;

    @BeforeEach
    void setUp() {
      readExecutor = mock(ReadExecutor.class);
      resolver =
          new LibraryReferenceResolver(
              readExecutor, mock(DataSource.class), mock(FhirEncoders.class));
    }

    @Test
    void resolvesLibrarySlashId() {
      final Library stored = newLibrary("abc", null, null, PublicationStatus.ACTIVE);
      when(readExecutor.read("Library", "abc")).thenReturn(stored);

      final IBaseResource resolved = resolver.resolve(new Reference("Library/abc"));

      assertThat(resolved).isSameAs(stored);
    }

    @Test
    void resolvesBareId() {
      final Library stored = newLibrary("abc", null, null, PublicationStatus.ACTIVE);
      when(readExecutor.read("Library", "abc")).thenReturn(stored);

      final IBaseResource resolved = resolver.resolve(new Reference("abc"));

      assertThat(resolved).isSameAs(stored);
    }

    @Test
    void rejectsReferenceToOtherResourceType() {
      final Reference reference = new Reference("Patient/abc");
      assertThatThrownBy(() -> resolver.resolve(reference))
          .isInstanceOf(InvalidRequestException.class)
          .hasMessageContaining("Library");
      verifyNoInteractions(readExecutor);
    }

    @Test
    void rejectsBlankReference() {
      final Reference reference = new Reference("");
      assertThatThrownBy(() -> resolver.resolve(reference))
          .isInstanceOf(InvalidRequestException.class)
          .hasMessageContaining("non-blank");
      verifyNoInteractions(readExecutor);
    }

    @Test
    void rejectsLibrarySlashWithEmptyId() {
      final Reference reference = new Reference("Library/");
      assertThatThrownBy(() -> resolver.resolve(reference))
          .isInstanceOf(InvalidRequestException.class)
          .hasMessageContaining("missing");
      verifyNoInteractions(readExecutor);
    }

    @Test
    void translatesResourceNotFoundErrorToResourceNotFoundException() {
      when(readExecutor.read("Library", "missing"))
          .thenThrow(new ResourceNotFoundError("not there"));
      final Reference reference = new Reference("Library/missing");

      assertThatThrownBy(() -> resolver.resolve(reference))
          .isInstanceOf(ResourceNotFoundException.class)
          .hasMessageContaining("missing");
    }

    @Test
    void translatesNoDataIllegalArgumentToResourceNotFoundException() {
      when(readExecutor.read("Library", "anyone"))
          .thenThrow(new IllegalArgumentException("No data found for resource type Library"));
      final Reference reference = new Reference("Library/anyone");

      assertThatThrownBy(() -> resolver.resolve(reference))
          .isInstanceOf(ResourceNotFoundException.class)
          .hasMessageContaining("anyone");
    }
  }

  // ---------------------------------------------------------------------------
  // Canonical references — uses a real Spark session + FhirEncoders.
  // ---------------------------------------------------------------------------

  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class CanonicalReferences {

    private SparkSession spark;
    private FhirEncoders fhirEncoders;
    private DataSource dataSource;
    private LibraryReferenceResolver resolver;

    @BeforeAll
    void setUpAll() {
      spark =
          SparkSession.builder()
              .master("local[1]")
              .appName("LibraryReferenceResolverTest")
              .config("spark.driver.bindAddress", "localhost")
              .config("spark.driver.host", "localhost")
              .config("spark.ui.enabled", false)
              .config("spark.sql.shuffle.partitions", 1)
              .getOrCreate();
      fhirEncoders = FhirEncoders.forR4().getOrCreate();
    }

    @AfterAll
    void tearDownAll() {
      if (spark != null) {
        spark.stop();
      }
    }

    @BeforeEach
    void setUp() {
      dataSource = mock(DataSource.class);
      resolver = new LibraryReferenceResolver(mock(ReadExecutor.class), dataSource, fhirEncoders);
    }

    @Test
    void resolvesByCanonicalUrl() {
      final Library stored =
          newLibrary("a", "https://example.org/Library/foo", "1.0", PublicationStatus.ACTIVE);
      when(dataSource.read("Library")).thenReturn(libraryDataset(stored));

      final IBaseResource resolved =
          resolver.resolve(new Reference("https://example.org/Library/foo"));

      assertThat(resolved).isInstanceOf(Library.class);
      assertThat(((Library) resolved).getId()).isEqualTo("Library/a");
      verify(dataSource).read("Library");
    }

    @Test
    void resolvesByCanonicalUrlWithVersion() {
      final Library v1 =
          newLibrary("a", "https://example.org/Library/foo", "1.0", PublicationStatus.ACTIVE);
      final Library v2 =
          newLibrary("b", "https://example.org/Library/foo", "2.0", PublicationStatus.ACTIVE);
      when(dataSource.read("Library")).thenReturn(libraryDataset(v1, v2));

      final IBaseResource resolved =
          resolver.resolve(new Reference("https://example.org/Library/foo|2.0"));

      assertThat(((Library) resolved).getId()).isEqualTo("Library/b");
    }

    @Test
    void prefersActiveOverDraftWhenNoVersionSupplied() {
      final Library draft =
          newLibrary("a", "https://example.org/Library/foo", "1.0", PublicationStatus.DRAFT);
      final Library active =
          newLibrary("b", "https://example.org/Library/foo", "1.0", PublicationStatus.ACTIVE);
      when(dataSource.read("Library")).thenReturn(libraryDataset(draft, active));

      final IBaseResource resolved =
          resolver.resolve(new Reference("https://example.org/Library/foo"));

      assertThat(((Library) resolved).getId()).isEqualTo("Library/b");
    }

    @Test
    void picksLatestActiveVersionWhenNoneSupplied() {
      final Library v1 =
          newLibrary("a", "https://example.org/Library/foo", "1.0", PublicationStatus.ACTIVE);
      final Library v2 =
          newLibrary("b", "https://example.org/Library/foo", "2.0", PublicationStatus.ACTIVE);
      when(dataSource.read("Library")).thenReturn(libraryDataset(v1, v2));

      final IBaseResource resolved =
          resolver.resolve(new Reference("https://example.org/Library/foo"));

      assertThat(((Library) resolved).getId()).isEqualTo("Library/b");
    }

    @Test
    void returns404WhenCanonicalDoesNotMatch() {
      when(dataSource.read("Library")).thenReturn(libraryDataset());
      final Reference reference = new Reference("https://example.org/Library/missing");

      assertThatThrownBy(() -> resolver.resolve(reference))
          .isInstanceOf(ResourceNotFoundException.class)
          .hasMessageContaining("missing");
    }

    @Test
    void returns404WhenVersionDoesNotMatch() {
      final Library v1 =
          newLibrary("a", "https://example.org/Library/foo", "1.0", PublicationStatus.ACTIVE);
      when(dataSource.read("Library")).thenReturn(libraryDataset(v1));
      final Reference reference = new Reference("https://example.org/Library/foo|99.0");

      assertThatThrownBy(() -> resolver.resolve(reference))
          .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void treatsUrnReferenceAsCanonical() {
      final Library stored = newLibrary("a", "urn:uuid:abc-123", "1.0", PublicationStatus.ACTIVE);
      when(dataSource.read("Library")).thenReturn(libraryDataset(stored));

      final IBaseResource resolved = resolver.resolve(new Reference("urn:uuid:abc-123"));

      assertThat(((Library) resolved).getId()).isEqualTo("Library/a");
    }

    private Dataset<Row> libraryDataset(final Library... libraries) {
      return spark.createDataset(List.of(libraries), fhirEncoders.of("Library")).toDF();
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers shared across nested classes.
  // ---------------------------------------------------------------------------

  private static Library newLibrary(
      final String id, final String url, final String version, final PublicationStatus status) {
    final Library library = new Library();
    library.setId("Library/" + id);
    if (url != null) {
      library.setUrl(url);
    }
    if (version != null) {
      library.setVersion(version);
    }
    library.setStatus(status);
    return library;
  }
}
