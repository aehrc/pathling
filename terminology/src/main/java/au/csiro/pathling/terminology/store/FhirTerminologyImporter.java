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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CANONICAL_URL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_MAP_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_EQUIVALENCE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ORDINAL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT_MAPPING;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.ENTRY_TYPE_CONCEPT_MAP;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.VALUE_SET;
import static org.apache.spark.sql.functions.col;

import au.csiro.pathling.terminology.store.CodeSystemStageLoader.StagedCodeSystem;
import au.csiro.pathling.terminology.store.TerminologyStoreWriter.CanonicalVersion;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.ValueSet;

/**
 * Imports FHIR R4 CodeSystem, ValueSet, and ConceptMap resources into the terminology store. The
 * source is read through the Hadoop FileSystem API and may be a single JSON file, a directory of
 * JSON files, or a FHIR NPM package ({@code .tgz}); Bundles are unwrapped.
 *
 * <p>Every source, including the entries of each Bundle, is first pre-scanned to validate cheap
 * structural facts (each importable resource is a FHIR object carrying a canonical URL) before
 * anything is written. Every resource is then read into driver-local NDJSON staging, one staging
 * per resource type, and only once the whole source has been read is the store written, with one
 * Delta commit per table for the whole import. A source that cannot be read therefore leaves the
 * store untouched, and the cost of an import does not grow with the number of resources it holds.
 * CodeSystems and ConceptMaps of any size, standalone or in a Bundle, are streamed into their
 * staging with bounded memory. ValueSets keep the whole-resource HAPI path, guarded by a size limit
 * so an oversized one fails with an actionable error rather than a memory error.
 *
 * <p>Where a source holds the same resource (by type, canonical URL and version) more than once,
 * the last occurrence is imported and the earlier ones are skipped with a warning.
 *
 * @author John Grimes
 */
@Slf4j
public class FhirTerminologyImporter {

  /**
   * The maximum byte size of a ValueSet, which is handled through the whole-resource HAPI path.
   * Comfortably below the JVM array limit and far above any legitimate ValueSet.
   */
  static final long DEFAULT_WHOLE_RESOURCE_LIMIT_BYTES = 1L << 30;

  private static final String CODE_SYSTEM_TYPE = "CodeSystem";

  private static final String VALUE_SET_TYPE = "ValueSet";

  private static final String CONCEPT_MAP_TYPE = "ConceptMap";

  /** The entry array of a Bundle. */
  private static final String FIELD_ENTRY = "entry";

  /** The field of a Bundle entry that holds its resource. */
  private static final String FIELD_RESOURCE = "resource";

  /**
   * The Parquet row-group size applied while writing store tables during import. A small, fixed
   * row-group bounds the memory each of the many concurrent Delta writers buffers, so peak write
   * memory stays within a modest driver heap regardless of the number of concepts. Without this
   * bound the writers default to 128 MB row groups and, multiplied across the driver's cores,
   * reserve almost the whole heap through the Parquet memory pool, starving the shuffle that feeds
   * the write and exhausting the heap on the largest tables.
   */
  static final int IMPORT_PARQUET_BLOCK_SIZE_BYTES = 16 * 1024 * 1024;

  /** The Hadoop configuration key controlling the Parquet row-group size. */
  private static final String PARQUET_BLOCK_SIZE_KEY = "parquet.block.size";

  private static final JsonFactory JSON_FACTORY = newFactory();

  private static FhirContext fhirContext;

  @Nonnull private final SparkSession spark;
  @Nonnull private final String storagePath;
  @Nonnull private final Configuration hadoopConf;
  private final long wholeResourceLimitBytes;

  /**
   * Creates an importer targeting a store.
   *
   * @param spark the Spark session used to write
   * @param storagePath the root path of the terminology store, created if absent
   */
  public FhirTerminologyImporter(
      @Nonnull final SparkSession spark, @Nonnull final String storagePath) {
    this(spark, storagePath, DEFAULT_WHOLE_RESOURCE_LIMIT_BYTES);
  }

  /**
   * Creates an importer with an explicit whole-resource size limit, for testing the guard.
   *
   * @param spark the Spark session used to write
   * @param storagePath the root path of the terminology store, created if absent
   * @param wholeResourceLimitBytes the maximum byte size of a ValueSet, the one resource imported
   *     whole
   */
  FhirTerminologyImporter(
      @Nonnull final SparkSession spark,
      @Nonnull final String storagePath,
      final long wholeResourceLimitBytes) {
    this.spark = spark;
    this.storagePath = storagePath;
    this.hadoopConf = spark.sessionState().newHadoopConf();
    this.wholeResourceLimitBytes = wholeResourceLimitBytes;
  }

  private static JsonFactory newFactory() {
    final JsonFactory factory = new JsonFactory();
    // A CodeSystem is streamed from a shared archive stream, so closing its parser must not close
    // the underlying stream.
    factory.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
    return factory;
  }

  @Nonnull
  private static synchronized IParser parser() {
    if (fhirContext == null) {
      fhirContext = FhirContext.forR4();
    }
    return fhirContext.newJsonParser();
  }

  /**
   * Imports FHIR terminology resources from a source.
   *
   * @param source a JSON file, a directory of JSON files, or a FHIR NPM package ({@code .tgz})
   * @param verifyPackage whether a package is checked against the checksum its registry publishes
   * @param registryUrl the registry to consult, or null for the default
   * @throws TerminologyImportException if the source contains no importable resources or an invalid
   *     resource, or if a package does not match the registry's checksum, in which case the store
   *     is left unmodified; or if writing the store fails, in which case the store may hold partial
   *     versions of the resources being imported until the import is re-run
   */
  public void importFrom(
      @Nonnull final String source,
      final boolean verifyPackage,
      @Nullable final String registryUrl) {
    // Pre-scan and validate before any write so an invalid source leaves the store untouched.
    final FhirSourceScan scan = new FhirResourceScanner(hadoopConf).scan(source);
    final List<ScannedResource> scanned = scan.getResources();
    validate(scanned, source);
    // The verification runs before anything is written, so a package that does not match its
    // registry leaves the store exactly as it was.
    final ImportProvenance provenance = resolveProvenance(scan, source, verifyPackage, registryUrl);
    // The pre-scan already established each entry's type, URL, and version, so the import pass
    // routes by looking these up rather than re-reading each entry's content into memory.
    final Map<String, ScannedResource> byEntry = new HashMap<>();
    for (final ScannedResource resource : scanned) {
      byEntry.put(resource.getEntryName(), resource);
    }

    try (ImportBatch batch = new ImportBatch(provenance, supersededOccurrences(scanned))) {
      try {
        stagePass(source, byEntry, batch);
      } catch (final IOException e) {
        throw new TerminologyImportException("Unable to read the FHIR source at " + source, e);
      }
      batch.seal();
      log.info(
          "Read {} code systems, {} value sets and {} concept maps from {}; writing the store",
          batch.codeSystems.size(),
          batch.valueSets.size(),
          batch.conceptMaps.size(),
          source);
      // Bound the Parquet row-group size for the duration of the writes so that concurrent Delta
      // writers buffer a fixed amount of memory, then restore the caller's configuration.
      final Configuration baseHadoopConf = spark.sparkContext().hadoopConfiguration();
      final String previousBlockSize =
          applyBoundedParquetRowGroup(baseHadoopConf, IMPORT_PARQUET_BLOCK_SIZE_BYTES);
      try {
        write(batch);
      } catch (final Exception e) {
        // Spark and Delta are written in Scala and may throw checked exceptions undeclared, such as
        // an IOException from the file system, so every exception is caught here.
        throw new TerminologyImportException(
            "The import from "
                + source
                + " failed after writing had begun. The store may hold partial versions of the"
                + " resources being imported; re-running the import with a corrected source will"
                + " repair it.",
            e);
      } finally {
        restoreParquetRowGroup(baseHadoopConf, previousBlockSize);
      }
      log.info(
          "FHIR import complete: {} code systems, {} value sets, {} concept maps",
          batch.codeSystems.size(),
          batch.valueSets.size(),
          batch.conceptMaps.size());
    }
  }

  /**
   * Establishes what this import records about where its content came from, consulting the registry
   * when the source is an identified package and the caller asked for the check.
   *
   * @param scan the pre-scan of the source, carrying its digests and package identity
   * @param source the source path, as passed to the import
   * @param verifyPackage whether a package is checked against the registry
   * @param registryUrl the registry to consult, or null for the default
   * @return the provenance every manifest row of this import carries
   * @throws TerminologyImportException if the package does not match the registry's checksum
   */
  @Nonnull
  private static ImportProvenance resolveProvenance(
      @Nonnull final FhirSourceScan scan,
      @Nonnull final String source,
      final boolean verifyPackage,
      @Nullable final String registryUrl) {
    if (scan.getSha256() != null) {
      log.info("Source {} has SHA-256 {}", source, scan.getSha256());
    }
    if (!scan.isPackage()) {
      return ImportProvenance.of(source, scan.getSha256());
    }
    final String name = scan.getPackageName();
    final String version = scan.getPackageVersion();
    final PackageVerificationResult result;
    if (name == null || version == null || scan.getSha1() == null) {
      // Without an identity there is nothing to ask the registry about.
      result = PackageVerificationResult.unverified("the package could not be identified");
      log.warn("The package at {} was not verified: {}", source, result.getReason());
    } else if (!verifyPackage) {
      result = new PackageVerificationResult(PackageVerification.SKIPPED, null, null);
      log.info("Package verification skipped for {} {}", name, version);
    } else {
      result = new PackageRegistryVerifier(registryUrl).verify(name, version, scan.getSha1());
      if (result.getStatus() == PackageVerification.VERIFIED) {
        log.info(
            "Verified package {} {} against {} (SHA-1 {})",
            name,
            version,
            result.getRegistry(),
            scan.getSha1());
      } else {
        log.warn("Package {} {} was not verified: {}", name, version, result.getReason());
      }
    }
    return new ImportProvenance(
        source, scan.getSha256(), name, version, result.getStatus(), result.getRegistry());
  }

  /**
   * Applies the bounded Parquet row-group size to a Hadoop configuration, returning the value it
   * replaced so it can be restored afterwards.
   *
   * @param hadoopConf the base Hadoop configuration the write path derives from
   * @param blockSizeBytes the bounded row-group size to apply
   * @return the prior {@code parquet.block.size} value, or null if it was unset
   */
  @Nullable
  static String applyBoundedParquetRowGroup(
      @Nonnull final Configuration hadoopConf, final int blockSizeBytes) {
    final String previous = hadoopConf.get(PARQUET_BLOCK_SIZE_KEY);
    hadoopConf.setInt(PARQUET_BLOCK_SIZE_KEY, blockSizeBytes);
    return previous;
  }

  /**
   * Restores a Hadoop configuration's Parquet row-group size to a previously captured value,
   * unsetting it when the prior value was absent.
   *
   * @param hadoopConf the base Hadoop configuration to restore
   * @param previous the value captured by {@link #applyBoundedParquetRowGroup}, or null if it was
   *     unset
   */
  static void restoreParquetRowGroup(
      @Nonnull final Configuration hadoopConf, @Nullable final String previous) {
    if (previous == null) {
      hadoopConf.unset(PARQUET_BLOCK_SIZE_KEY);
    } else {
      hadoopConf.set(PARQUET_BLOCK_SIZE_KEY, previous);
    }
  }

  /**
   * Validates every terminology resource of a source, including the members of each Bundle, before
   * anything is written: each must carry a canonical URL, and a ValueSet, the one resource still
   * read whole, must fit within the whole-resource limit.
   */
  private void validate(
      @Nonnull final List<ScannedResource> scanned, @Nonnull final String source) {
    final List<ScannedResource> resources = terminologyResources(scanned).toList();
    if (resources.isEmpty()) {
      throw new TerminologyImportException(
          "No importable FHIR CodeSystem, ValueSet, or ConceptMap resources were found in "
              + source
              + ".");
    }
    for (final ScannedResource resource : resources) {
      requireUrl(resource.getUrl(), resource.getResourceType(), resource.getEntryName());
      if (VALUE_SET_TYPE.equals(resource.getResourceType())
          && resource.getByteSize() > wholeResourceLimitBytes) {
        throw new TerminologyImportException(
            "The ValueSet "
                + resource.getUrl()
                + " in "
                + resource.getEntryName()
                + " is "
                + resource.getByteSize()
                + " bytes, exceeding the "
                + wholeResourceLimitBytes
                + "-byte limit on a ValueSet, which is imported whole; only CodeSystems and"
                + " ConceptMaps are imported with bounded memory.");
      }
    }
  }

  /** Returns every terminology resource of a source in source order, Bundle members in place. */
  @Nonnull
  private static Stream<ScannedResource> terminologyResources(
      @Nonnull final List<ScannedResource> scanned) {
    return scanned.stream()
        .flatMap(
            resource -> resource.isBundle() ? resource.getMembers().stream() : Stream.of(resource))
        .filter(ScannedResource::isTerminologyResource);
  }

  /**
   * Finds the occurrences of a resource that a later occurrence of the same resource in the source
   * supersedes, so that only the last is imported, as it would be if each were written in turn. A
   * resource is identified by what keys it in the store: a CodeSystem by its system version, a
   * ConceptMap by its concept map identifier, and a ValueSet by its canonical URL and version.
   *
   * @param scanned the pre-scan of the source
   * @return the superseded occurrences, compared by identity
   */
  @Nonnull
  private static Set<ScannedResource> supersededOccurrences(
      @Nonnull final List<ScannedResource> scanned) {
    final Map<List<String>, ScannedResource> last = new LinkedHashMap<>();
    final Set<ScannedResource> superseded = Collections.newSetFromMap(new IdentityHashMap<>());
    terminologyResources(scanned)
        .forEach(
            resource -> {
              final ScannedResource earlier = last.put(storeKey(resource), resource);
              if (earlier != null) {
                superseded.add(earlier);
                log.warn(
                    "{} {}{} in {} is repeated in {}; only the later occurrence is imported",
                    resource.getResourceType(),
                    resource.getUrl(),
                    resource.getVersion() == null ? "" : " version " + resource.getVersion(),
                    earlier.getEntryName(),
                    resource.getEntryName());
              }
            });
    return superseded;
  }

  /** The key under which the store holds a terminology resource. */
  @Nonnull
  private static List<String> storeKey(@Nonnull final ScannedResource resource) {
    final String type = resource.getResourceType();
    final String url = resource.getUrl();
    final String version = resource.getVersion();
    if (CODE_SYSTEM_TYPE.equals(type)) {
      return List.of(
          type, TerminologyStoreSchema.systemVersionId(url, version == null ? "" : version));
    }
    if (CONCEPT_MAP_TYPE.equals(type)) {
      return List.of(type, TerminologyStoreSchema.conceptMapId(url, version));
    }
    // A ValueSet is keyed by the version HAPI parses, which reads an empty version as absent.
    return Arrays.asList(type, url, version == null || version.isEmpty() ? null : version);
  }

  private static void requireUrl(
      @Nullable final String url,
      @Nullable final String resourceType,
      @Nonnull final String entryName) {
    if (url == null || url.isBlank()) {
      throw new TerminologyImportException(
          "A "
              + resourceType
              + " resource in "
              + entryName
              + " is missing its canonical url and cannot be imported.");
    }
  }

  // --- Stage pass: read every resource into staging, writing nothing to the store. ---

  private void stagePass(
      @Nonnull final String source,
      @Nonnull final Map<String, ScannedResource> byEntry,
      @Nonnull final ImportBatch batch)
      throws IOException {
    final Path root = new Path(source);
    final FileSystem fs = root.getFileSystem(hadoopConf);
    if (fs.getFileStatus(root).isDirectory()) {
      final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(root, true);
      while (iterator.hasNext()) {
        final LocatedFileStatus status = iterator.next();
        final String name = status.getPath().getName();
        if (name.endsWith(".json") && !FhirResourceScanner.isPackageMetadata(name)) {
          try (InputStream in = fs.open(status.getPath())) {
            stageEntry(in, byEntry.get(status.getPath().toString()), batch);
          }
        }
      }
    } else if (FhirResourceScanner.isPackage(source)) {
      try (TarArchiveInputStream tar =
          new TarArchiveInputStream(new GzipCompressorInputStream(fs.open(root)))) {
        TarArchiveEntry entry;
        while ((entry = tar.getNextEntry()) != null) {
          final String name = new Path(entry.getName()).getName();
          if (!entry.isDirectory()
              && name.endsWith(".json")
              && !FhirResourceScanner.isPackageMetadata(name)) {
            stageEntry(tar, byEntry.get(entry.getName()), batch);
          }
        }
      }
    } else {
      try (InputStream in = fs.open(root)) {
        stageEntry(in, byEntry.get(source), batch);
      }
    }
  }

  /**
   * Stages one JSON entry, routing by the pre-scan result. A CodeSystem, a ConceptMap, and a Bundle
   * are streamed straight from the entry stream with no whole-entry buffering, while a ValueSet,
   * bounded by the pre-scan, is read into memory for the HAPI path. The stream is positioned at the
   * start of the entry; for an archive it reports end-of-entry so the streaming parser never reads
   * past the entry, and it is not closed here.
   */
  private void stageEntry(
      @Nonnull final InputStream in,
      @Nullable final ScannedResource scanned,
      @Nonnull final ImportBatch batch)
      throws IOException {
    if (scanned == null || !scanned.isImportable() || batch.superseded.contains(scanned)) {
      return;
    }
    if (VALUE_SET_TYPE.equals(scanned.getResourceType())) {
      stageValueSet(new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8), scanned, batch);
      return;
    }
    try (JsonParser parser = JSON_FACTORY.createParser(in)) {
      if (scanned.isBundle()) {
        stageBundle(parser, scanned, batch);
      } else {
        stageStreamed(parser, scanned, batch);
      }
    }
  }

  /**
   * Walks a Bundle's entries in the order the pre-scan recorded them, staging the resource of each
   * entry whose member is terminology content through the same path as a standalone resource.
   */
  private void stageBundle(
      @Nonnull final JsonParser parser,
      @Nonnull final ScannedResource bundle,
      @Nonnull final ImportBatch batch)
      throws IOException {
    if (parser.nextToken() != JsonToken.START_OBJECT) {
      throw new TerminologyImportException(
          "Expected a Bundle JSON object in " + bundle.getEntryName());
    }
    final List<ScannedResource> members = bundle.getMembers();
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      parser.nextToken();
      if (!FIELD_ENTRY.equals(field) || parser.currentToken() != JsonToken.START_ARRAY) {
        parser.skipChildren();
        continue;
      }
      int index = 0;
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        if (index >= members.size()) {
          throw new TerminologyImportException(
              "The Bundle in " + bundle.getEntryName() + " changed while it was being imported.");
        }
        stageBundleEntry(parser, members.get(index++), batch);
      }
    }
  }

  /** Stages the resource of one Bundle entry, skipping everything else in the entry. */
  private void stageBundleEntry(
      @Nonnull final JsonParser parser,
      @Nonnull final ScannedResource member,
      @Nonnull final ImportBatch batch)
      throws IOException {
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      parser.skipChildren();
      return;
    }
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      if (!FIELD_RESOURCE.equals(parser.currentName())
          || !member.isTerminologyResource()
          || batch.superseded.contains(member)) {
        parser.nextToken();
        parser.skipChildren();
      } else if (VALUE_SET_TYPE.equals(member.getResourceType())) {
        parser.nextToken();
        stageValueSet(copyObject(parser), member, batch);
      } else {
        // The streaming paths start by reading the resource's opening brace.
        stageStreamed(parser, member, batch);
      }
    }
  }

  /** Copies the JSON object at the parser's position into a string, consuming it. */
  @Nonnull
  private static String copyObject(@Nonnull final JsonParser parser) throws IOException {
    final StringWriter json = new StringWriter();
    try (JsonGenerator generator = JSON_FACTORY.createGenerator(json)) {
      generator.copyCurrentStructure(parser);
    }
    return json.toString();
  }

  /**
   * Stages a CodeSystem or a ConceptMap from a parser positioned before its opening brace,
   * consuming the resource.
   */
  private void stageStreamed(
      @Nonnull final JsonParser parser,
      @Nonnull final ScannedResource scanned,
      @Nonnull final ImportBatch batch) {
    if (scanned.isCodeSystem()) {
      stageCodeSystem(parser, scanned.getUrl(), scanned.getVersion(), batch);
    } else {
      stageConceptMap(parser, scanned.getUrl(), scanned.getVersion(), batch);
    }
  }

  /** Flattens a CodeSystem through the streaming path into the CodeSystem staging. */
  private static void stageCodeSystem(
      @Nonnull final JsonParser parser,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final ImportBatch batch) {
    log.info("Streaming CodeSystem {}", url);
    batch.codeSystemStaging.beginCodeSystem(
        TerminologyStoreSchema.systemVersionId(url, version == null ? "" : version));
    final CodeSystemStreamFlattener flattener =
        new CodeSystemStreamFlattener(batch.codeSystemStaging);
    try {
      flattener.flatten(parser);
    } catch (final IOException | RuntimeException e) {
      throw new TerminologyImportException(
          "Unable to parse CodeSystem "
              + url
              + " from "
              + batch.provenance.getSource()
              + "; the source may be corrupt.",
          e);
    }
    batch.codeSystems.add(new StagedCodeSystem(url, version, flattener.getHierarchyMeaning()));
  }

  /**
   * Flattens a ConceptMap through the streaming path into the ConceptMap staging. A map that cannot
   * be read is rejected before anything is written.
   */
  private static void stageConceptMap(
      @Nonnull final JsonParser parser,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final ImportBatch batch) {
    log.info("Streaming ConceptMap {}", url);
    final ConceptMapStaging staging = batch.conceptMapStaging;
    final String conceptMapId = TerminologyStoreSchema.conceptMapId(url, version);
    staging.beginConceptMap(conceptMapId);
    final int mappings;
    try {
      mappings = new ConceptMapStreamFlattener(staging).flatten(parser);
    } catch (final IOException | RuntimeException e) {
      throw new TerminologyImportException(
          "Unable to read ConceptMap "
              + url
              + " from "
              + batch.provenance.getSource()
              + ": "
              + e.getMessage(),
          e);
    }
    log.info("Read ConceptMap {} ({} mappings)", url, mappings);
    // The reason the map cannot be read as a concept map relation, if any, describes the version as
    // a whole, so it is recorded with the version rather than among its mappings.
    batch.conceptMaps.add(new StagedConceptMap(conceptMapId, url, version, staging.fault()));
  }

  /** Stages a ValueSet, already bounded by the pre-scan, through the whole-resource HAPI path. */
  private static void stageValueSet(
      @Nonnull final String json,
      @Nonnull final ScannedResource scanned,
      @Nonnull final ImportBatch batch) {
    final IBaseResource parsed;
    try {
      parsed = parser().parseResource(json);
    } catch (final DataFormatException e) {
      throw new TerminologyImportException(
          "Unable to parse FHIR resource from " + scanned.getEntryName() + ": " + e.getMessage(),
          e);
    }
    if (!(parsed instanceof final ValueSet valueSet)) {
      throw new TerminologyImportException(
          "Expected a ValueSet in " + scanned.getEntryName() + " but found " + parsed.fhirType());
    }
    requireUrl(valueSet.getUrl(), VALUE_SET_TYPE, scanned.getEntryName());
    batch.valueSetStaging.append(
        valueSet.getUrl(), valueSet.getVersion(), parser().encodeResourceToString(valueSet));
    batch.valueSets.add(new CanonicalVersion(valueSet.getUrl(), valueSet.getVersion()));
  }

  // --- Write: one commit per table, the manifest last. ---

  /**
   * Writes the staged content of an import to the store, each table once, and the manifest last so
   * that a failure mid-write leaves no version recorded as imported.
   */
  private void write(@Nonnull final ImportBatch batch) {
    final TerminologyStoreWriter writer = new TerminologyStoreWriter(spark, storagePath);
    if (!batch.codeSystems.isEmpty()) {
      new CodeSystemStageLoader(spark, writer).load(batch.codeSystemStaging, batch.codeSystems);
    }
    if (!batch.conceptMaps.isEmpty()) {
      log.info("Loading {} concept map(s) into the store", batch.conceptMaps.size());
      writer.writePartitions(
          conceptMappings(batch),
          CONCEPT_MAPPING,
          COLUMN_CONCEPT_MAP_ID,
          batch.conceptMaps.stream().map(StagedConceptMap::conceptMapId).toList());
    }
    if (!batch.valueSets.isEmpty()) {
      log.info("Loading {} value set(s) into the store", batch.valueSets.size());
      writer.writeByCanonicalVersion(batch.valueSetStaging.read(spark), VALUE_SET, batch.valueSets);
    }
    writer.upsertManifestEntries(manifestEntries(batch));
  }

  /** Reads the staged mappings of every ConceptMap as rows of the concept mapping table. */
  @Nonnull
  private Dataset<Row> conceptMappings(@Nonnull final ImportBatch batch) {
    final String mapId = "map_concept_map_id";
    final StructType mapSchema =
        new StructType()
            .add(mapId, DataTypes.StringType, false)
            .add(COLUMN_CANONICAL_URL, DataTypes.StringType, false)
            .add(COLUMN_VERSION, DataTypes.StringType, true);
    final Dataset<Row> maps =
        spark.createDataFrame(
            batch.conceptMaps.stream()
                .map(map -> RowFactory.create(map.conceptMapId(), map.url(), map.version()))
                .toList(),
            mapSchema);
    return batch
        .conceptMapStaging
        .read(spark)
        .join(maps, col(COLUMN_CONCEPT_MAP_ID).equalTo(col(mapId)))
        .select(
            col(COLUMN_CANONICAL_URL),
            col(COLUMN_VERSION),
            col(COLUMN_ORDINAL),
            col(COLUMN_SOURCE_SYSTEM),
            col(COLUMN_SOURCE_VERSION),
            col(COLUMN_SOURCE_CODE),
            col(COLUMN_SOURCE_DISPLAY),
            col(COLUMN_TARGET_SYSTEM),
            col(COLUMN_TARGET_VERSION),
            col(COLUMN_TARGET_CODE),
            col(COLUMN_TARGET_DISPLAY),
            col(COLUMN_EQUIVALENCE),
            col(COLUMN_CONCEPT_MAP_ID));
  }

  /** Builds the manifest entry of every resource the import writes. */
  @Nonnull
  private static List<ManifestEntry> manifestEntries(@Nonnull final ImportBatch batch) {
    final Instant importedAt = Instant.now();
    final ImportProvenance provenance = batch.provenance;
    final List<ManifestEntry> entries = new ArrayList<>();
    for (final StagedCodeSystem codeSystem : batch.codeSystems) {
      entries.add(
          ManifestEntry.forImport(
              "code_system", codeSystem.url(), codeSystem.version(), provenance, importedAt));
    }
    for (final CanonicalVersion valueSet : batch.valueSets) {
      entries.add(
          ManifestEntry.forImport(
              "value_set", valueSet.url(), valueSet.version(), provenance, importedAt));
    }
    for (final StagedConceptMap conceptMap : batch.conceptMaps) {
      entries.add(
          ManifestEntry.forImport(
              ENTRY_TYPE_CONCEPT_MAP,
              conceptMap.url(),
              conceptMap.version(),
              provenance,
              importedAt,
              conceptMap.fault()));
    }
    return entries;
  }

  /** A ConceptMap staged for writing, with the fault recorded while it was read, if any. */
  private record StagedConceptMap(
      @Nonnull String conceptMapId,
      @Nonnull String url,
      @Nullable String version,
      @Nullable String fault) {}

  /**
   * The staging of every resource of one import, and what is known about each staged resource, held
   * until the whole source has been read and the store is written.
   */
  private static final class ImportBatch implements AutoCloseable {

    @Nonnull private final ImportProvenance provenance;
    @Nonnull private final Set<ScannedResource> superseded;
    @Nonnull private final CodeSystemStaging codeSystemStaging;
    @Nonnull private final ConceptMapStaging conceptMapStaging;
    @Nonnull private final ValueSetStaging valueSetStaging;
    @Nonnull private final List<StagedCodeSystem> codeSystems = new ArrayList<>();
    @Nonnull private final List<StagedConceptMap> conceptMaps = new ArrayList<>();
    @Nonnull private final List<CanonicalVersion> valueSets = new ArrayList<>();

    private ImportBatch(
        @Nonnull final ImportProvenance provenance,
        @Nonnull final Set<ScannedResource> superseded) {
      this.provenance = provenance;
      this.superseded = superseded;
      codeSystemStaging = CodeSystemStaging.create();
      try {
        conceptMapStaging = ConceptMapStaging.create();
      } catch (final RuntimeException e) {
        codeSystemStaging.close();
        throw e;
      }
      try {
        valueSetStaging = ValueSetStaging.create();
      } catch (final RuntimeException e) {
        codeSystemStaging.close();
        conceptMapStaging.close();
        throw e;
      }
    }

    /** Seals every staging for reading. */
    private void seal() {
      codeSystemStaging.sealForReading();
      conceptMapStaging.sealForReading();
      valueSetStaging.sealForReading();
    }

    @Override
    public void close() {
      codeSystemStaging.close();
      conceptMapStaging.close();
      valueSetStaging.close();
    }
  }
}
