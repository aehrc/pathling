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

package au.csiro.pathling.terminology.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the streaming pre-scan: it reads the leading metadata and byte size of a resource in
 * each source shape, stops before a large trailing content array, reports a missing canonical URL,
 * and skips package metadata entries.
 *
 * @author John Grimes
 */
class FhirResourceScannerTest {

  @Test
  void extractsMetadataFromABareStream() throws Exception {
    final String json =
        "{\"resourceType\":\"CodeSystem\",\"url\":\"http://x/cs\",\"version\":\"2.1\"}";
    final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

    final ScannedResource scanned =
        FhirResourceScanner.scanStream(new ByteArrayInputStream(bytes), "cs.json", bytes.length);

    assertEquals("CodeSystem", scanned.getResourceType());
    assertEquals("http://x/cs", scanned.getUrl());
    assertEquals("2.1", scanned.getVersion());
    assertEquals("cs.json", scanned.getEntryName());
    assertEquals(bytes.length, scanned.getByteSize());
    assertTrue(scanned.isCodeSystem());
  }

  @Test
  void extractsMetadataFromADirectoryMember(@TempDir final Path dir) throws Exception {
    Files.copy(FhirPackageFixtures.resource("nested-hierarchy.json"), dir.resolve("nested.json"));

    final List<ScannedResource> scanned =
        new FhirResourceScanner(new Configuration()).scan(dir.toString());

    assertEquals(1, scanned.size());
    assertEquals("CodeSystem", scanned.get(0).getResourceType());
    assertEquals("http://example.org/fhir/CodeSystem/nested", scanned.get(0).getUrl());
    assertEquals("1.0.0", scanned.get(0).getVersion());
    assertTrue(scanned.get(0).getByteSize() > 0);
  }

  @Test
  void extractsMetadataFromAPackageEntry(@TempDir final Path dir) throws Exception {
    final Path archive =
        FhirPackageFixtures.buildPackage(
            dir, "cs.tgz", "nested-hierarchy.json", "valueset-simple.json");

    final List<ScannedResource> scanned =
        new FhirResourceScanner(new Configuration()).scan(archive.toString());

    final Map<String, ScannedResource> byType =
        scanned.stream().collect(Collectors.toMap(ScannedResource::getResourceType, s -> s));
    assertEquals(2, byType.size());
    assertEquals("http://example.org/fhir/CodeSystem/nested", byType.get("CodeSystem").getUrl());
    assertEquals("http://example.org/fhir/ValueSet/simple", byType.get("ValueSet").getUrl());
    assertTrue(byType.get("CodeSystem").getByteSize() > 0);
  }

  @Test
  void stopsBeforeReadingALargeConceptArray() throws Exception {
    final StringBuilder concepts = new StringBuilder();
    for (int i = 0; i < 5000; i++) {
      if (i > 0) {
        concepts.append(',');
      }
      concepts
          .append("{\"code\":\"c")
          .append(i)
          .append("\",\"display\":\"Concept ")
          .append(i)
          .append("\"}");
    }
    final String json =
        "{\"resourceType\":\"CodeSystem\",\"url\":\"http://x/cs\",\"version\":\"1\",\"concept\":["
            + concepts
            + "]}";
    final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    // The document is large, but the metadata that must be read sits in its first few dozen bytes.
    assertTrue(bytes.length > 100_000);

    final CountingStream counting = new CountingStream(new ByteArrayInputStream(bytes));
    final ScannedResource scanned =
        FhirResourceScanner.scanStream(counting, "cs.json", bytes.length);

    assertEquals("CodeSystem", scanned.getResourceType());
    assertEquals("http://x/cs", scanned.getUrl());
    assertEquals("1", scanned.getVersion());
    // Far less than the whole document was consumed: the scan stopped at the concept array,
    // reading at most Jackson's initial input buffer rather than the whole 100 KB document.
    assertTrue(
        counting.getByteCount() < 32_768,
        "expected the scan to read far less than the whole document, read "
            + counting.getByteCount()
            + " of "
            + bytes.length);
  }

  @Test
  void reportsAMissingUrlAsNull() throws Exception {
    final byte[] bytes =
        FhirPackageFixtures.read("codesystem-no-url.json").getBytes(StandardCharsets.UTF_8);

    final ScannedResource scanned =
        FhirResourceScanner.scanStream(
            new ByteArrayInputStream(bytes), "codesystem-no-url.json", bytes.length);

    assertEquals("CodeSystem", scanned.getResourceType());
    assertNull(scanned.getUrl());
  }

  @Test
  void skipsPackageMetadataEntries(@TempDir final Path dir) throws Exception {
    // The package helper always writes a package.json metadata entry, which must not be scanned.
    final Path archive = FhirPackageFixtures.buildPackage(dir, "cs.tgz", "nested-hierarchy.json");

    final List<ScannedResource> scanned =
        new FhirResourceScanner(new Configuration()).scan(archive.toString());

    assertEquals(1, scanned.size());
    assertEquals("CodeSystem", scanned.get(0).getResourceType());
  }

  /** A stream that counts the bytes read through it, for asserting the pre-scan's early exit. */
  private static final class CountingStream extends FilterInputStream {

    private long count;

    CountingStream(final InputStream in) {
      super(in);
    }

    long getByteCount() {
      return count;
    }

    @Override
    public int read() throws IOException {
      final int value = super.read();
      if (value != -1) {
        count++;
      }
      return value;
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
      final int read = super.read(buffer, offset, length);
      if (read > 0) {
        count += read;
      }
      return read;
    }
  }
}
