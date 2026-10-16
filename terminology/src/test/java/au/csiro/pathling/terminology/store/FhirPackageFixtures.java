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
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;

/**
 * Builds FHIR NPM packages ({@code .tgz}) from the JSON fixtures under {@code
 * terminology/src/test/resources/fhir-import/} at test runtime, so the streaming import can be
 * exercised in its package form without checking binary archives into the repository. A package
 * carries a {@code package.json} metadata entry unless one is explicitly withheld, and the importer
 * must skip it.
 *
 * @author John Grimes
 */
public final class FhirPackageFixtures {

  private static final String FIXTURE_ROOT = "/fhir-import/";

  /** The package name recorded in the {@code package.json} of every package built by default. */
  public static final String PACKAGE_NAME = "fixtures";

  /** The package version recorded in the {@code package.json} of every default package. */
  public static final String PACKAGE_VERSION = "1.0.0";

  /** The {@code package.json} content written into a package unless another is supplied. */
  public static final String DEFAULT_PACKAGE_JSON =
      "{\"name\":\"" + PACKAGE_NAME + "\",\"version\":\"" + PACKAGE_VERSION + "\"}";

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
   * package/} directory, alongside a {@code package.json} metadata entry naming {@link
   * #PACKAGE_NAME} and {@link #PACKAGE_VERSION}.
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
    return buildPackageWithJson(directory, archiveName, DEFAULT_PACKAGE_JSON, fixtureNames);
  }

  /**
   * Builds a {@code .tgz} package from the named fixtures with an explicit {@code package.json}
   * content, or none at all. This method carries a distinct name rather than overloading {@link
   * #buildPackage}, whose trailing variable arity parameter would make the two calls ambiguous.
   *
   * @param directory the directory to write the archive into
   * @param archiveName the archive file name (for example {@code fixtures.tgz})
   * @param packageJson the content of the {@code package.json} entry, or null to omit the entry
   * @param fixtureNames the fixture file names to include, in the order they should appear
   * @return the path to the created archive
   * @throws IOException if the archive cannot be written
   */
  @Nonnull
  public static Path buildPackageWithJson(
      @Nonnull final Path directory,
      @Nonnull final String archiveName,
      @Nullable final String packageJson,
      @Nonnull final String... fixtureNames)
      throws IOException {
    final Path archive = directory.resolve(archiveName);
    try (TarArchiveOutputStream tar =
        new TarArchiveOutputStream(
            new GzipCompressorOutputStream(Files.newOutputStream(archive)))) {
      tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      if (packageJson != null) {
        writeEntry(tar, "package/package.json", packageJson);
      }
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
   * Rewrites a gzipped tarball with a different compression level, yielding a readable package with
   * exactly the same content but different bytes. A byte flipped inside the gzip payload would
   * instead fail as a corrupt archive, before any checksum is compared.
   *
   * @param archive the archive to rewrite
   * @return the path to the rewritten archive, a sibling of the original
   * @throws IOException if the archive cannot be read or written
   */
  @Nonnull
  public static Path recompress(@Nonnull final Path archive) throws IOException {
    final byte[] uncompressed;
    try (InputStream in = new GzipCompressorInputStream(Files.newInputStream(archive))) {
      uncompressed = in.readAllBytes();
    }
    final Path rewritten = archive.resolveSibling("recompressed-" + archive.getFileName());
    final GzipParameters parameters = new GzipParameters();
    parameters.setCompressionLevel(1);
    try (OutputStream out =
        new GzipCompressorOutputStream(Files.newOutputStream(rewritten), parameters)) {
      out.write(uncompressed);
    }
    return rewritten;
  }

  /**
   * Computes the SHA-1 of a file, as the registry publishes it.
   *
   * @param file the file to digest
   * @return the digest as lowercase hexadecimal
   * @throws IOException if the file cannot be read
   */
  @Nonnull
  public static String sha1Hex(@Nonnull final Path file) throws IOException {
    return digestHex(file, "SHA-1");
  }

  /**
   * Computes the SHA-256 of a file, as the manifest records it.
   *
   * @param file the file to digest
   * @return the digest as lowercase hexadecimal
   * @throws IOException if the file cannot be read
   */
  @Nonnull
  public static String sha256Hex(@Nonnull final Path file) throws IOException {
    return digestHex(file, "SHA-256");
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

  @Nonnull
  private static String digestHex(@Nonnull final Path file, @Nonnull final String algorithm)
      throws IOException {
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance(algorithm);
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException(algorithm + " is not available", e);
    }
    try (InputStream in = Files.newInputStream(file)) {
      final byte[] buffer = new byte[8192];
      int read;
      while ((read = in.read(buffer)) != -1) {
        digest.update(buffer, 0, read);
      }
    }
    final StringBuilder builder = new StringBuilder();
    for (final byte b : digest.digest()) {
      builder.append(String.format("%02x", b));
    }
    return builder.toString();
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
