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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT_MAPPING;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.ENTRY_TYPE_CONCEPT_MAP;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.VALUE_SET;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.ValueSet;

/**
 * Imports FHIR R4 CodeSystem, ValueSet, and ConceptMap resources into the terminology store. The
 * source is read through the Hadoop FileSystem API and may be a single JSON file, a directory of
 * JSON files, or a FHIR NPM package ({@code .tgz}); Bundles are unwrapped.
 *
 * <p>Every source, including the entries of each Bundle, is first pre-scanned to validate cheap
 * structural facts (each importable resource is a FHIR object carrying a canonical URL) before
 * anything is written, so an invalid source leaves the store untouched. CodeSystems and ConceptMaps
 * of any size, standalone or in a Bundle, are then streamed through a bounded-memory pipeline
 * (token-stream flatten to temporary NDJSON staging, then a Spark load), so peak driver memory does
 * not grow with the number of concepts or mappings. ValueSets keep the whole-resource HAPI path,
 * guarded by a size limit so an oversized one fails with an actionable error rather than a memory
 * error.
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

  private static final String VALUE_SET_TYPE = "ValueSet";

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
   *     resource, or if a package does not match the registry's checksum; the store is left
   *     unmodified
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

    final TerminologyStoreWriter writer = new TerminologyStoreWriter(spark, storagePath);
    final CodeSystemStageLoader loader = new CodeSystemStageLoader(spark, writer);
    final ImportCounts counts = new ImportCounts();
    // Bound the Parquet row-group size for the duration of the writes so that concurrent Delta
    // writers buffer a fixed amount of memory, then restore the caller's configuration.
    final Configuration baseHadoopConf = spark.sparkContext().hadoopConfiguration();
    final String previousBlockSize =
        applyBoundedParquetRowGroup(baseHadoopConf, IMPORT_PARQUET_BLOCK_SIZE_BYTES);
    try {
      importPass(provenance, byEntry, writer, loader, counts);
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to read the FHIR source at " + source, e);
    } finally {
      restoreParquetRowGroup(baseHadoopConf, previousBlockSize);
    }
    log.info(
        "FHIR import complete: {} code systems, {} value sets, {} concept maps",
        counts.codeSystems,
        counts.valueSets,
        counts.conceptMaps);
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
    final List<ScannedResource> resources =
        scanned.stream()
            .flatMap(
                resource ->
                    resource.isBundle() ? resource.getMembers().stream() : Stream.of(resource))
            .filter(ScannedResource::isTerminologyResource)
            .toList();
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

  // --- Import pass. ---

  private void importPass(
      @Nonnull final ImportProvenance provenance,
      @Nonnull final Map<String, ScannedResource> byEntry,
      @Nonnull final TerminologyStoreWriter writer,
      @Nonnull final CodeSystemStageLoader loader,
      @Nonnull final ImportCounts counts)
      throws IOException {
    final String source = provenance.getSource();
    final Path root = new Path(source);
    final FileSystem fs = root.getFileSystem(hadoopConf);
    final ImportContext context = new ImportContext(provenance, writer, loader, counts);
    if (fs.getFileStatus(root).isDirectory()) {
      final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(root, true);
      while (iterator.hasNext()) {
        final LocatedFileStatus status = iterator.next();
        final String name = status.getPath().getName();
        if (name.endsWith(".json") && !FhirResourceScanner.isPackageMetadata(name)) {
          try (InputStream in = fs.open(status.getPath())) {
            importEntry(in, byEntry.get(status.getPath().toString()), context);
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
            importEntry(tar, byEntry.get(entry.getName()), context);
          }
        }
      }
    } else {
      try (InputStream in = fs.open(root)) {
        importEntry(in, byEntry.get(source), context);
      }
    }
  }

  /**
   * Imports one JSON entry, routing by the pre-scan result. A CodeSystem, a ConceptMap, and a
   * Bundle are streamed straight from the entry stream with no whole-entry buffering, while a
   * ValueSet, bounded by the pre-scan, is read into memory for the HAPI path. The stream is
   * positioned at the start of the entry; for an archive it reports end-of-entry so the streaming
   * parser never reads past the entry, and it is not closed here.
   */
  private void importEntry(
      @Nonnull final InputStream in,
      @Nullable final ScannedResource scanned,
      @Nonnull final ImportContext context)
      throws IOException {
    if (scanned == null || !scanned.isImportable()) {
      return;
    }
    if (VALUE_SET_TYPE.equals(scanned.getResourceType())) {
      importValueSet(new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8), scanned, context);
      return;
    }
    try (JsonParser parser = JSON_FACTORY.createParser(in)) {
      if (scanned.isBundle()) {
        importBundle(parser, scanned, context);
      } else {
        importStreamed(parser, scanned, context);
      }
    }
  }

  /**
   * Walks a Bundle's entries in the order the pre-scan recorded them, importing the resource of
   * each entry whose member is terminology content through the same path as a standalone resource.
   */
  private void importBundle(
      @Nonnull final JsonParser parser,
      @Nonnull final ScannedResource bundle,
      @Nonnull final ImportContext context)
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
        importBundleEntry(parser, members.get(index++), context);
      }
    }
  }

  /** Imports the resource of one Bundle entry, skipping everything else in the entry. */
  private void importBundleEntry(
      @Nonnull final JsonParser parser,
      @Nonnull final ScannedResource member,
      @Nonnull final ImportContext context)
      throws IOException {
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      parser.skipChildren();
      return;
    }
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      if (!FIELD_RESOURCE.equals(parser.currentName()) || !member.isTerminologyResource()) {
        parser.nextToken();
        parser.skipChildren();
      } else if (VALUE_SET_TYPE.equals(member.getResourceType())) {
        parser.nextToken();
        importValueSet(copyObject(parser), member, context);
      } else {
        // The streaming paths start by reading the resource's opening brace.
        importStreamed(parser, member, context);
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
   * Imports a CodeSystem or a ConceptMap from a parser positioned before its opening brace,
   * consuming the resource.
   */
  private void importStreamed(
      @Nonnull final JsonParser parser,
      @Nonnull final ScannedResource scanned,
      @Nonnull final ImportContext context) {
    if (scanned.isCodeSystem()) {
      flattenAndLoad(parser, scanned.getUrl(), scanned.getVersion(), context);
    } else {
      flattenAndWriteConceptMap(parser, scanned.getUrl(), scanned.getVersion(), context);
    }
  }

  /**
   * Flattens a CodeSystem through the streaming path and loads it, translating failures into the
   * partial-version contract once a write has begun.
   */
  private void flattenAndLoad(
      @Nonnull final JsonParser parser,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final ImportContext context) {
    log.info("Streaming CodeSystem {}", url);
    final ImportCounts counts = context.counts();
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      final CodeSystemStreamFlattener flattener = new CodeSystemStreamFlattener(staging);
      try {
        flattener.flatten(parser);
      } catch (final IOException | RuntimeException e) {
        if (counts.writeBegun) {
          throw partialFailure("CodeSystem", url, version, e);
        }
        throw new TerminologyImportException(
            "Unable to parse CodeSystem "
                + url
                + " from "
                + context.provenance().getSource()
                + "; the source may be corrupt.",
            e);
      }
      staging.sealForReading();
      counts.writeBegun = true;
      try {
        context
            .loader()
            .load(staging, url, version, flattener.getHierarchyMeaning(), context.provenance());
      } catch (final RuntimeException e) {
        throw partialFailure("CodeSystem", url, version, e);
      }
    }
    counts.codeSystems++;
  }

  /**
   * Flattens a ConceptMap through the streaming path into staging, then replaces the stored
   * mappings of its version with the staged ones. A map that cannot be read is rejected before any
   * of it is written.
   */
  private void flattenAndWriteConceptMap(
      @Nonnull final JsonParser parser,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final ImportContext context) {
    log.info("Streaming ConceptMap {}", url);
    final ImportCounts counts = context.counts();
    try (ConceptMapStaging staging = ConceptMapStaging.create()) {
      final int mappings;
      try {
        mappings = new ConceptMapStreamFlattener(staging).flatten(parser);
      } catch (final IOException | RuntimeException e) {
        throw new TerminologyImportException(
            "Unable to read ConceptMap "
                + url
                + " from "
                + context.provenance().getSource()
                + ": "
                + e.getMessage(),
            e);
      }
      staging.sealForReading();
      counts.writeBegun = true;
      log.info("Loading ConceptMap {} ({} mappings) into the store", url, mappings);
      try {
        writeConceptMapping(staging, url, version, context);
      } catch (final RuntimeException e) {
        throw partialFailure("ConceptMap", url, version, e);
      }
    }
    counts.conceptMaps++;
  }

  private void writeConceptMapping(
      @Nonnull final ConceptMapStaging staging,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final ImportContext context) {
    final String conceptMapId = TerminologyStoreSchema.conceptMapId(url, version);
    final Dataset<Row> data =
        staging
            .read(spark)
            .select(
                lit(url).alias(COLUMN_CANONICAL_URL),
                lit(version).cast(DataTypes.StringType).alias(COLUMN_VERSION),
                col(COLUMN_ORDINAL),
                col(COLUMN_SOURCE_SYSTEM),
                col(COLUMN_SOURCE_CODE),
                col(COLUMN_TARGET_SYSTEM),
                col(COLUMN_TARGET_CODE),
                col(COLUMN_EQUIVALENCE),
                lit(conceptMapId).alias(COLUMN_CONCEPT_MAP_ID));
    final TerminologyStoreWriter writer = context.writer();
    if (writer.tableExists(CONCEPT_MAPPING)) {
      writer.replaceWhere(
          data, CONCEPT_MAPPING, COLUMN_CONCEPT_MAP_ID + " = '" + conceptMapId + "'");
    } else {
      writer.writeTable(data, CONCEPT_MAPPING, SaveMode.Overwrite, List.of(COLUMN_CONCEPT_MAP_ID));
    }
    writer.upsertManifestEntry(
        ManifestEntry.forImport(
            ENTRY_TYPE_CONCEPT_MAP, url, version, context.provenance(), Instant.now()));
  }

  @Nonnull
  private static TerminologyImportException partialFailure(
      @Nonnull final String resourceType,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final Throwable cause) {
    return new TerminologyImportException(
        "The import of "
            + resourceType
            + " "
            + url
            + (version != null ? " version " + version : "")
            + " failed after writing had begun. The store may hold a partial version of it;"
            + " re-running the import with a corrected source will repair it.",
        cause);
  }

  /** Imports a ValueSet, already bounded by the pre-scan, through the whole-resource HAPI path. */
  private void importValueSet(
      @Nonnull final String json,
      @Nonnull final ScannedResource scanned,
      @Nonnull final ImportContext context) {
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
    context.counts().writeBegun = true;
    final Dataset<Row> data =
        spark.createDataFrame(
            List.of(
                RowFactory.create(
                    valueSet.getUrl(),
                    valueSet.getVersion(),
                    parser().encodeResourceToString(valueSet))),
            TerminologyStoreSchema.resourceTableSchema());
    final TerminologyStoreWriter writer = context.writer();
    if (writer.tableExists(VALUE_SET)) {
      writer.replaceWhere(
          data,
          VALUE_SET,
          COLUMN_CANONICAL_URL
              + " = '"
              + valueSet.getUrl()
              + "' AND "
              + TerminologyStoreWriter.versionPredicate(valueSet.getVersion()));
    } else {
      writer.writeTable(data, VALUE_SET, SaveMode.Overwrite, List.of());
    }
    writer.upsertManifestEntry(
        ManifestEntry.forImport(
            "value_set",
            valueSet.getUrl(),
            valueSet.getVersion(),
            context.provenance(),
            Instant.now()));
    context.counts().valueSets++;
  }

  /** What every resource of one import pass is written with and counted against. */
  private record ImportContext(
      @Nonnull ImportProvenance provenance,
      @Nonnull TerminologyStoreWriter writer,
      @Nonnull CodeSystemStageLoader loader,
      @Nonnull ImportCounts counts) {}

  /** Mutable running counts and the write-begun flag across an import. */
  private static final class ImportCounts {
    int codeSystems;
    int valueSets;
    int conceptMaps;
    boolean writeBegun;
  }
}
