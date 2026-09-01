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

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

/**
 * Builds FHIR NPM packages ({@code .tgz}) from the JSON fixtures under {@code
 * terminology/src/test/resources/fhir-import/} at test runtime, so the streaming import can be
 * exercised in its package form without checking binary archives into the repository. Every package
 * carries a {@code package.json} metadata entry, which the importer must skip.
 *
 * @author John Grimes
 */
public final class FhirPackageFixtures {

  private static final String FIXTURE_ROOT = "/fhir-import/";

  private FhirPackageFixtures() {
    // Test helper.
  }

  /**
   * Resolves a fixture file on the classpath by its file name.
   *
   * @param fixtureName the fixture file name (for example {@code nested-hierarchy.json})
   * @return the path to the fixture file
   * @throws IllegalStateException if the fixture is not on the classpath
   */
  @Nonnull
  public static Path resource(@Nonnull final String fixtureName) {
    final URL url = FhirPackageFixtures.class.getResource(FIXTURE_ROOT + fixtureName);
    if (url == null) {
      throw new IllegalStateException("FHIR import fixture not found on classpath: " + fixtureName);
    }
    return Paths.get(url.getPath());
  }

  /**
   * Builds a {@code .tgz} package from the named fixtures, each stored under the package's {@code
   * package/} directory, alongside a {@code package.json} metadata entry.
   *
   * @param directory the directory to write the archive into
   * @param archiveName the archive file name (for example {@code fixtures.tgz})
   * @param fixtureNames the fixture file names to include, in the order they should appear
   * @return the path to the created archive
   * @throws IOException if the archive cannot be written
   */
  @Nonnull
  public static Path buildPackage(
      @Nonnull final Path directory,
      @Nonnull final String archiveName,
      @Nonnull final String... fixtureNames)
      throws IOException {
    final Path archive = directory.resolve(archiveName);
    try (TarArchiveOutputStream tar =
        new TarArchiveOutputStream(
            new GzipCompressorOutputStream(Files.newOutputStream(archive)))) {
      tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      writeEntry(tar, "package/package.json", "{\"name\":\"fixtures\",\"version\":\"1.0.0\"}");
      for (final String fixtureName : fixtureNames) {
        final byte[] content = Files.readAllBytes(resource(fixtureName));
        writeEntry(tar, "package/" + fixtureName, content);
      }
    }
    return archive;
  }

  /**
   * Builds a package containing a valid CodeSystem followed by a non-CodeSystem (ValueSet) entry
   * padded to an artificially large size, for exercising the whole-resource size guard. The
   * ValueSet is enlarged with filler text so that a modest injected size limit is exceeded.
   *
   * @param directory the directory to write the archive into
   * @return the path to the created archive
   * @throws IOException if the archive cannot be written
   */
  @Nonnull
  public static Path buildGuardPackage(@Nonnull final Path directory) throws IOException {
    final Path archive = directory.resolve("guard.tgz");
    final String valueSet = enlargedValueSet();
    try (TarArchiveOutputStream tar =
        new TarArchiveOutputStream(
            new GzipCompressorOutputStream(Files.newOutputStream(archive)))) {
      tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      writeEntry(tar, "package/package.json", "{\"name\":\"guard\",\"version\":\"1.0.0\"}");
      writeEntry(
          tar, "package/simple-valid.json", Files.readAllBytes(resource("simple-valid.json")));
      writeEntry(tar, "package/valueset-large.json", valueSet);
    }
    return archive;
  }

  /**
   * Reads a fixture file as a UTF-8 string.
   *
   * @param fixtureName the fixture file name
   * @return the fixture content
   */
  @Nonnull
  public static String read(@Nonnull final String fixtureName) {
    try {
      return Files.readString(resource(fixtureName));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Produces a ValueSet with a large filler {@code text} narrative to inflate its byte size. */
  @Nonnull
  private static String enlargedValueSet() {
    final String filler = "x".repeat(4096);
    return "{\"resourceType\":\"ValueSet\",\"url\":\"http://example.org/fhir/ValueSet/large\","
               + "\"version\":\"1.0.0\",\"status\":\"active\",\"text\":{\"status\":\"generated\",\"div\":\"<div"
               + " xmlns=\\\"http://www.w3.org/1999/xhtml\\\">"
        + filler
        + "</div>\"}}";
  }

  private static void writeEntry(
      @Nonnull final TarArchiveOutputStream tar,
      @Nonnull final String name,
      @Nonnull final String content)
      throws IOException {
    writeEntry(tar, name, content.getBytes(StandardCharsets.UTF_8));
  }

  private static void writeEntry(
      @Nonnull final TarArchiveOutputStream tar,
      @Nonnull final String name,
      @Nonnull final byte[] content)
      throws IOException {
    final TarArchiveEntry entry = new TarArchiveEntry(name);
    entry.setSize(content.length);
    tar.putArchiveEntry(entry);
    final OutputStream out = tar;
    out.write(content);
    tar.closeArchiveEntry();
  }
}
