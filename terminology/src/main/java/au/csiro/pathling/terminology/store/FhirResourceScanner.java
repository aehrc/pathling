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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Streams each FHIR resource in a source just far enough to read its metadata ({@code
 * resourceType}, {@code url}, {@code version}) and byte size, stopping before a CodeSystem's large
 * concept array. A Bundle is read through, scanning the resource of each of its entries as a member
 * of the Bundle. This lets the importer validate cheap structural facts and route resources by type
 * and size before writing anything, with peak memory independent of the source size.
 *
 * <p>The scan handles the same three source shapes as the importer: a bare JSON file, a directory
 * of JSON files, and a FHIR NPM package ({@code .tgz}). A package's {@code package.json} is read
 * for the package identity rather than scanned as a resource, and other metadata entries are
 * skipped. For an archive the scan is a single streamed pass; the importer reads the archive a
 * second time for the content pass, since a gzip stream is not seekable.
 *
 * <p>The same pass digests a single-file source, so its SHA-1 and SHA-256 are known before the
 * import writes anything.
 *
 * @author John Grimes
 */
@Slf4j
public class FhirResourceScanner {

  /** The metadata fields the pre-scan collects before stopping at the first large content array. */
  private static final String FIELD_RESOURCE_TYPE = "resourceType";

  private static final String FIELD_URL = "url";
  private static final String FIELD_VERSION = "version";

  /** The large array field that marks the end of the metadata of a CodeSystem. */
  private static final String FIELD_CONCEPT = "concept";

  /** The entry array of a Bundle, whose resources are scanned as members of the Bundle. */
  private static final String FIELD_ENTRY = "entry";

  /** The field of a Bundle entry that holds its resource. */
  private static final String FIELD_RESOURCE = "resource";

  /** The package manifest field naming the package, alongside the shared {@code version} field. */
  private static final String FIELD_NAME = "name";

  /** The name of the package manifest entry, which carries the package identity. */
  private static final String PACKAGE_MANIFEST = "package.json";

  private static final JsonFactory FACTORY = newFactory();

  private static JsonFactory newFactory() {
    final JsonFactory factory = new JsonFactory();
    // The scan reads one entry at a time from a shared archive stream, so closing a per-entry
    // parser must never close the underlying stream.
    factory.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
    return factory;
  }

  @Nonnull private final Configuration hadoopConf;

  /**
   * Creates a scanner.
   *
   * @param hadoopConf the Hadoop configuration used to open the source
   */
  public FhirResourceScanner(@Nonnull final Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  /**
   * Scans every resource in a source, returning their metadata without reading their content.
   *
   * @param source a JSON file, a directory of JSON files, or a FHIR NPM package ({@code .tgz})
   * @return the scanned resources with the digests and package identity of the source
   * @throws TerminologyImportException if the source does not exist or cannot be read
   */
  @Nonnull
  public FhirSourceScan scan(@Nonnull final String source) {
    final Path root = new Path(source);
    final List<ScannedResource> scanned = new ArrayList<>();
    log.info("Scanning FHIR terminology source {}", source);
    try {
      final FileSystem fs = root.getFileSystem(hadoopConf);
      if (!fs.exists(root)) {
        throw new TerminologyImportException("FHIR source path does not exist: " + source);
      }
      if (fs.getFileStatus(root).isDirectory()) {
        // A directory is a set of files with no single set of bytes to hash.
        scanDirectory(fs, root, scanned);
        return new FhirSourceScan(scanned, null, null, null, null, false);
      }
      if (isPackage(source)) {
        return scanPackage(fs, root, scanned);
      }
      try (DigestingInputStream in = new DigestingInputStream(fs.open(root))) {
        scanned.add(scanStream(in, source, fs.getFileStatus(root).getLen()));
        // The pre-scan stops at the first large content array, so the rest of the file is pulled
        // through the digests here.
        in.drain();
        return new FhirSourceScan(scanned, in.sha1Hex(), in.sha256Hex(), null, null, false);
      }
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to read the FHIR source at " + source, e);
    }
  }

  private void scanDirectory(
      @Nonnull final FileSystem fs,
      @Nonnull final Path root,
      @Nonnull final List<ScannedResource> scanned)
      throws IOException {
    final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(root, true);
    while (iterator.hasNext()) {
      final LocatedFileStatus status = iterator.next();
      final String name = status.getPath().getName();
      if (name.endsWith(".json") && !isPackageMetadata(name)) {
        try (InputStream in = fs.open(status.getPath())) {
          scanned.add(scanStream(in, status.getPath().toString(), status.getLen()));
        }
      }
    }
  }

  @Nonnull
  private FhirSourceScan scanPackage(
      @Nonnull final FileSystem fs,
      @Nonnull final Path root,
      @Nonnull final List<ScannedResource> scanned)
      throws IOException {
    String packageName = null;
    String packageVersion = null;
    try (DigestingInputStream digesting = new DigestingInputStream(fs.open(root))) {
      // Closing the tar and gzip readers must not close the digesting stream, which still has the
      // bytes trailing the last entry to give up.
      try (TarArchiveInputStream tar =
          new TarArchiveInputStream(
              new GzipCompressorInputStream(CloseShieldInputStream.wrap(digesting)))) {
        TarArchiveEntry entry;
        while ((entry = tar.getNextEntry()) != null) {
          if (entry.isDirectory()) {
            continue;
          }
          final String name = new Path(entry.getName()).getName();
          if (name.equals(PACKAGE_MANIFEST)) {
            final String[] identity = readPackageIdentity(tar);
            packageName = identity[0];
            packageVersion = identity[1];
          } else if (name.endsWith(".json") && !isPackageMetadata(name)) {
            // The tar input stream reports end-of-entry, so Jackson never reads past the entry
            // boundary; any unread bytes of an early-exited entry are skipped by the next call to
            // getNextEntry.
            scanned.add(scanStream(tar, entry.getName(), entry.getSize()));
          }
        }
      }
      // The gzip and tar readers stop at the end of the archive's last entry, so the trailing
      // padding and any bytes beyond it are pulled through the digests here.
      digesting.drain();
      return new FhirSourceScan(
          scanned, digesting.sha1Hex(), digesting.sha256Hex(), packageName, packageVersion, true);
    }
  }

  /**
   * Reads the {@code name} and {@code version} of a package from its {@code package.json} entry,
   * leaving either null when the field is absent.
   *
   * @param in the {@code package.json} entry stream, which is not closed
   * @return the name at index zero and the version at index one
   * @throws IOException if the entry cannot be read
   */
  @Nonnull
  private static String[] readPackageIdentity(@Nonnull final InputStream in) throws IOException {
    final String[] identity = new String[2];
    try (JsonParser parser = FACTORY.createParser(in)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        return identity;
      }
      while (parser.nextToken() == JsonToken.FIELD_NAME) {
        final String field = parser.currentName();
        parser.nextToken();
        switch (field) {
          case FIELD_NAME -> identity[0] = parser.getValueAsString();
          case FIELD_VERSION -> identity[1] = parser.getValueAsString();
          default -> parser.skipChildren();
        }
      }
    } catch (final JsonProcessingException e) {
      // A package.json that is not readable JSON leaves the package unidentified, exactly as an
      // absent one does; the import then records it as unverified.
      log.warn("Unable to read the package.json of the package being imported", e);
    }
    return identity;
  }

  /**
   * Scans a single resource stream, reading only its metadata. A Bundle's entries are scanned in
   * turn, each resource just as a standalone one is, so that the Bundle is returned with a member
   * per entry. The stream is not closed.
   *
   * @param in the resource JSON stream
   * @param entryName the file path or archive entry name, for routing and error messages
   * @param byteSize the byte size of the entry
   * @return the scanned resource; its {@code resourceType}, {@code url}, or {@code version} are
   *     null when absent from the metadata read
   * @throws IOException if the stream cannot be read
   */
  @Nonnull
  public static ScannedResource scanStream(
      @Nonnull final InputStream in, @Nonnull final String entryName, final long byteSize)
      throws IOException {
    try (JsonParser parser = FACTORY.createParser(in)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        return new ScannedResource(null, null, null, entryName, byteSize);
      }
      final ResourceMetadata metadata = new ResourceMetadata();
      final List<ScannedResource> members = new ArrayList<>();
      boolean scanning = true;
      while (scanning && parser.nextToken() == JsonToken.FIELD_NAME) {
        final String field = parser.currentName();
        parser.nextToken();
        if (FIELD_ENTRY.equals(field)) {
          // Only a Bundle has an entry array, though its resourceType may not have been read yet.
          scanEntries(parser, entryName, members);
        } else if (!FIELD_CONCEPT.equals(field)) {
          metadata.read(field, parser);
        }
        // The concept array of a CodeSystem is its large content array; stop before reading it so
        // the scan cost stays a few kilobytes. A complete non-Bundle needs nothing more.
        scanning = !FIELD_CONCEPT.equals(field) && !(metadata.isComplete() && !metadata.isBundle());
      }
      return metadata.toScannedResource(
          entryName, byteSize, metadata.isBundle() ? members : List.of());
    }
  }

  /** Scans the entries of a Bundle, adding a member for each, in order. */
  private static void scanEntries(
      @Nonnull final JsonParser parser,
      @Nonnull final String entryName,
      @Nonnull final List<ScannedResource> members)
      throws IOException {
    if (parser.currentToken() != JsonToken.START_ARRAY) {
      parser.skipChildren();
      return;
    }
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      final String memberName = entryName + "#entry[" + members.size() + "]";
      ScannedResource member = new ScannedResource(null, null, null, memberName, 0);
      if (parser.currentToken() == JsonToken.START_OBJECT) {
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
          final String field = parser.currentName();
          parser.nextToken();
          if (FIELD_RESOURCE.equals(field) && parser.currentToken() == JsonToken.START_OBJECT) {
            member = scanMember(parser, memberName);
          } else {
            parser.skipChildren();
          }
        }
      } else {
        parser.skipChildren();
      }
      members.add(member);
    }
  }

  /**
   * Scans the resource of a Bundle entry, from its opening brace to its closing one, measuring its
   * byte size from the parser's offsets. Unlike a standalone resource, the whole object is
   * consumed, since the scan continues with the next entry.
   */
  @Nonnull
  private static ScannedResource scanMember(
      @Nonnull final JsonParser parser, @Nonnull final String memberName) throws IOException {
    final long start = parser.currentTokenLocation().getByteOffset();
    final ResourceMetadata metadata = new ResourceMetadata();
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      parser.nextToken();
      metadata.read(field, parser);
    }
    final long end = parser.currentLocation().getByteOffset();
    return metadata.toScannedResource(memberName, end - start, List.of());
  }

  /** The metadata fields of a resource, collected as the scan meets them. */
  private static final class ResourceMetadata {

    private String resourceType;
    private String url;
    private String version;

    /** Records a metadata field, or skips the value of any other field. */
    void read(@Nonnull final String field, @Nonnull final JsonParser parser) throws IOException {
      switch (field) {
        case FIELD_RESOURCE_TYPE -> resourceType = parser.getValueAsString();
        case FIELD_URL -> url = parser.getValueAsString();
        case FIELD_VERSION -> version = parser.getValueAsString();
        default -> parser.skipChildren();
      }
    }

    boolean isComplete() {
      return resourceType != null && url != null && version != null;
    }

    boolean isBundle() {
      return "Bundle".equals(resourceType);
    }

    @Nonnull
    ScannedResource toScannedResource(
        @Nonnull final String entryName,
        final long byteSize,
        @Nonnull final List<ScannedResource> members) {
      return new ScannedResource(resourceType, url, version, entryName, byteSize, members);
    }
  }

  /** Reports whether a source points at a FHIR NPM package by its file extension. */
  static boolean isPackage(@Nonnull final String source) {
    final String lower = source.toLowerCase();
    return lower.endsWith(".tgz") || lower.endsWith(".tar.gz");
  }

  /** Excludes the package manifest and index, which are not FHIR resources. */
  static boolean isPackageMetadata(@Nonnull final String name) {
    return name.equals(PACKAGE_MANIFEST) || name.startsWith(".");
  }
}
