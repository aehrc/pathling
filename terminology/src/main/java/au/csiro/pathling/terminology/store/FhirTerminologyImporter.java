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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT_MAP;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.VALUE_SET;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
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
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ValueSet;

/**
 * Imports FHIR R4 CodeSystem, ValueSet, and ConceptMap resources into the terminology store. The
 * source is read through the Hadoop FileSystem API and may be a single JSON file, a directory of
 * JSON files, or a FHIR NPM package ({@code .tgz}); Bundles are unwrapped.
 *
 * <p>Every source is first pre-scanned to validate cheap structural facts (each importable resource
 * is a FHIR object carrying a canonical URL) before anything is written, so an invalid source
 * leaves the store untouched. CodeSystems of any size are then streamed through a bounded-memory
 * pipeline (token-stream flatten to temporary NDJSON staging, then a Spark load), so peak driver
 * memory does not grow with the number of concepts. ValueSets and ConceptMaps keep the
 * whole-resource HAPI path, guarded by a size limit so an oversized one fails with an actionable
 * error rather than a memory error.
 *
 * @author John Grimes
 */
@Slf4j
public class FhirTerminologyImporter {

  /**
   * The maximum byte size of a resource handled through the whole-resource HAPI path. Comfortably
   * below the JVM array limit and far above any legitimate ValueSet or ConceptMap.
   */
  static final long DEFAULT_WHOLE_RESOURCE_LIMIT_BYTES = 1L << 30;

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
   * @param wholeResourceLimitBytes the maximum byte size for a whole-resource (non-CodeSystem)
   *     import
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
   * @throws TerminologyImportException if the source contains no importable resources or an invalid
   *     resource; the store is left unmodified
   */
  public void importFrom(@Nonnull final String source) {
    // Pre-scan and validate before any write so an invalid source leaves the store untouched.
    final List<ScannedResource> scanned = new FhirResourceScanner(hadoopConf).scan(source);
    validate(scanned, source);

    final TerminologyStoreWriter writer = new TerminologyStoreWriter(spark, storagePath);
    final CodeSystemStageLoader loader = new CodeSystemStageLoader(spark, writer);
    final ImportCounts counts = new ImportCounts();
    try {
      importPass(source, writer, loader, counts);
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to read the FHIR source at " + source, e);
    }
    log.info(
        "FHIR import complete: {} code systems, {} value sets, {} concept maps",
        counts.codeSystems,
        counts.valueSets,
        counts.conceptMaps);
  }

  private void validate(
      @Nonnull final List<ScannedResource> scanned, @Nonnull final String source) {
    boolean anyImportable = false;
    for (final ScannedResource resource : scanned) {
      if (!resource.isImportable()) {
        continue;
      }
      anyImportable = true;
      // A Bundle's canonical URL and its contents are validated when it is parsed on the import
      // pass, since the pre-scan does not descend into its entries.
      if (!"Bundle".equals(resource.getResourceType())) {
        requireUrl(resource.getUrl(), resource.getResourceType(), resource.getEntryName());
      }
      if (!resource.isCodeSystem() && resource.getByteSize() > wholeResourceLimitBytes) {
        throw new TerminologyImportException(
            "The "
                + resource.getResourceType()
                + " "
                + resource.getUrl()
                + " in "
                + resource.getEntryName()
                + " is "
                + resource.getByteSize()
                + " bytes, exceeding the "
                + wholeResourceLimitBytes
                + "-byte whole-resource import limit; only CodeSystems are imported with bounded"
                + " memory.");
      }
    }
    if (!anyImportable) {
      throw new TerminologyImportException(
          "No importable FHIR CodeSystem, ValueSet, or ConceptMap resources were found in "
              + source
              + ".");
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
      @Nonnull final String source,
      @Nonnull final TerminologyStoreWriter writer,
      @Nonnull final CodeSystemStageLoader loader,
      @Nonnull final ImportCounts counts)
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
            importFile(in, status.getPath().toString(), source, writer, loader, counts);
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
            importFile(tar, entry.getName(), source, writer, loader, counts);
          }
        }
      }
    } else {
      try (InputStream in = fs.open(root)) {
        importFile(in, source, source, writer, loader, counts);
      }
    }
  }

  /**
   * Imports one JSON entry, routing a CodeSystem through the streaming path and any other resource
   * through the whole-resource HAPI path. The entry is buffered so its resource type can be sniffed
   * before routing; the stream is not closed here.
   */
  private void importFile(
      @Nonnull final InputStream in,
      @Nonnull final String entryName,
      @Nonnull final String source,
      @Nonnull final TerminologyStoreWriter writer,
      @Nonnull final CodeSystemStageLoader loader,
      @Nonnull final ImportCounts counts)
      throws IOException {
    final byte[] bytes = IOUtils.toByteArray(in);
    final ScannedResource scanned = scanBytes(bytes, entryName);
    if (scanned.isCodeSystem()) {
      requireUrl(scanned.getUrl(), "CodeSystem", entryName);
      flattenAndLoad(bytes, scanned.getUrl(), scanned.getVersion(), source, loader, counts);
    } else if (scanned.isImportable()) {
      importWholeResource(bytes, entryName, source, writer, loader, counts);
    }
  }

  /**
   * Flattens a CodeSystem from its JSON bytes through the streaming path and loads it, translating
   * failures into the partial-version contract once a write has begun.
   */
  private void flattenAndLoad(
      @Nonnull final byte[] bytes,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final String source,
      @Nonnull final CodeSystemStageLoader loader,
      @Nonnull final ImportCounts counts) {
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      final CodeSystemStreamFlattener flattener = new CodeSystemStreamFlattener(staging);
      try (JsonParser parser = JSON_FACTORY.createParser(new java.io.ByteArrayInputStream(bytes))) {
        flattener.flatten(parser);
      } catch (final IOException | RuntimeException e) {
        if (counts.writeBegun) {
          throw partialFailure(url, version, e);
        }
        throw new TerminologyImportException(
            "Unable to parse CodeSystem "
                + url
                + " from "
                + source
                + "; the source may be corrupt.",
            e);
      }
      staging.sealForReading();
      counts.writeBegun = true;
      try {
        loader.load(staging, url, version, flattener.getHierarchyMeaning(), source);
      } catch (final RuntimeException e) {
        throw partialFailure(url, version, e);
      }
    }
    counts.codeSystems++;
  }

  @Nonnull
  private static TerminologyImportException partialFailure(
      @Nonnull final String url, @Nullable final String version, @Nonnull final Throwable cause) {
    return new TerminologyImportException(
        "The import of CodeSystem "
            + url
            + (version != null ? " version " + version : "")
            + " failed after writing had begun. The store may hold a partial version of it;"
            + " re-running the import with a corrected source will repair it.",
        cause);
  }

  /** Imports a ValueSet, ConceptMap, or Bundle through the whole-resource HAPI path. */
  private void importWholeResource(
      @Nonnull final byte[] bytes,
      @Nonnull final String entryName,
      @Nonnull final String source,
      @Nonnull final TerminologyStoreWriter writer,
      @Nonnull final CodeSystemStageLoader loader,
      @Nonnull final ImportCounts counts) {
    final IBaseResource parsed;
    try {
      parsed = parser().parseResource(new String(bytes, StandardCharsets.UTF_8));
    } catch (final DataFormatException e) {
      throw new TerminologyImportException(
          "Unable to parse FHIR resource from " + entryName + ": " + e.getMessage(), e);
    }
    if (parsed instanceof final Bundle bundle) {
      for (final Bundle.BundleEntryComponent bundleEntry : bundle.getEntry()) {
        final Resource resource = bundleEntry.getResource();
        if (resource != null) {
          importBundleResource(resource, entryName, source, writer, loader, counts);
        }
      }
    } else if (parsed instanceof final Resource resource) {
      importBundleResource(resource, entryName, source, writer, loader, counts);
    }
  }

  private void importBundleResource(
      @Nonnull final Resource resource,
      @Nonnull final String entryName,
      @Nonnull final String source,
      @Nonnull final TerminologyStoreWriter writer,
      @Nonnull final CodeSystemStageLoader loader,
      @Nonnull final ImportCounts counts) {
    if (resource instanceof final CodeSystem codeSystem) {
      requireUrl(codeSystem.getUrl(), "CodeSystem", entryName);
      // Re-encode the Bundle-extracted CodeSystem so it flows through the same streaming flattener.
      final byte[] json =
          parser().encodeResourceToString(codeSystem).getBytes(StandardCharsets.UTF_8);
      flattenAndLoad(json, codeSystem.getUrl(), codeSystem.getVersion(), source, loader, counts);
    } else if (resource instanceof final ValueSet valueSet) {
      requireUrl(valueSet.getUrl(), "ValueSet", entryName);
      counts.writeBegun = true;
      importResource(
          writer,
          VALUE_SET,
          "value_set",
          valueSet.getUrl(),
          valueSet.getVersion(),
          valueSet,
          source);
      counts.valueSets++;
    } else if (resource instanceof final ConceptMap conceptMap) {
      requireUrl(conceptMap.getUrl(), "ConceptMap", entryName);
      counts.writeBegun = true;
      importResource(
          writer,
          CONCEPT_MAP,
          "concept_map",
          conceptMap.getUrl(),
          conceptMap.getVersion(),
          conceptMap,
          source);
      counts.conceptMaps++;
    }
  }

  @Nonnull
  private static ScannedResource scanBytes(
      @Nonnull final byte[] bytes, @Nonnull final String entryName) {
    try {
      return FhirResourceScanner.scanStream(
          new java.io.ByteArrayInputStream(bytes), entryName, bytes.length);
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to read resource metadata from " + entryName, e);
    }
  }

  // --- Whole-resource storage (unchanged from the pre-streaming importer). ---

  private void importResource(
      @Nonnull final TerminologyStoreWriter writer,
      @Nonnull final String tableName,
      @Nonnull final String entryType,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final Resource resource,
      @Nonnull final String source) {
    final String json = parser().encodeResourceToString(resource);
    final Dataset<Row> data =
        spark.createDataFrame(
            List.of(RowFactory.create(url, version, json)),
            TerminologyStoreSchema.resourceTableSchema());
    if (writer.tableExists(tableName)) {
      writer.replaceWhere(
          data,
          tableName,
          COLUMN_CANONICAL_URL
              + " = '"
              + url
              + "' AND "
              + TerminologyStoreWriter.versionPredicate(version));
    } else {
      writer.writeTable(data, tableName, SaveMode.Overwrite, List.of());
    }
    writer.upsertManifestEntry(
        new ManifestEntry(
            TerminologyStoreSchema.STORE_FORMAT_VERSION,
            entryType,
            url,
            version,
            source,
            Instant.now()));
  }

  /** Mutable running counts and the write-begun flag across an import. */
  private static final class ImportCounts {
    int codeSystems;
    int valueSets;
    int conceptMaps;
    boolean writeBegun;
  }
}
