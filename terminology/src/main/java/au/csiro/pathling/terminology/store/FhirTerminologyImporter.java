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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CLOSURE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CODE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ACCEPTABILITY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ACTIVE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CANONICAL_URL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_COUNT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DEFINED;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_EFFECTIVE_TIME;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_HIERARCHY_MEANING;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_LANGUAGE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_MODULE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_PROPERTY_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ROLE_GROUP;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SNOMED_EDITION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SNOMED_EFFECTIVE_TIME;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TERM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TYPE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TYPE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_URL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VALUE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VALUE_TYPE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT_MAP;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.DESCRIPTION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.PROPERTY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.RELATIONSHIP;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.VALUE_SET;
import static org.apache.spark.sql.functions.lit;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.CodeSystem.ConceptDefinitionComponent;
import org.hl7.fhir.r4.model.CodeSystem.ConceptDefinitionDesignationComponent;
import org.hl7.fhir.r4.model.CodeSystem.ConceptPropertyComponent;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.ValueSet;

/**
 * Imports FHIR R4 CodeSystem, ValueSet, and ConceptMap resources into the terminology store. The
 * source is read through the Hadoop FileSystem API and may be a single JSON file, a directory of
 * JSON files, or a FHIR NPM package ({@code .tgz}); bundles are unwrapped. CodeSystems are
 * flattened into the same content tables as SNOMED CT (concepts, descriptions, scalar and
 * Coding-valued properties, and the transitive closure of the nested {@code is-a} hierarchy), while
 * ValueSets and ConceptMaps are stored as JSON keyed by canonical URL and version.
 *
 * <p>The source is fully parsed and validated before any table is written, so a source that
 * contains no importable resources, or a resource without a canonical URL, fails without modifying
 * the store.
 *
 * @author John Grimes
 */
@Slf4j
public class FhirTerminologyImporter {

  private static final String DESIGNATION_USAGE_SYSTEM =
      "http://terminology.hl7.org/CodeSystem/designation-usage";

  private static FhirContext fhirContext;

  @Nonnull private final SparkSession spark;
  @Nonnull private final String storagePath;
  @Nonnull private final Configuration hadoopConf;

  /**
   * Creates an importer targeting a store.
   *
   * @param spark the Spark session used to write
   * @param storagePath the root path of the terminology store, created if absent
   */
  public FhirTerminologyImporter(
      @Nonnull final SparkSession spark, @Nonnull final String storagePath) {
    this.spark = spark;
    this.storagePath = storagePath;
    this.hadoopConf = spark.sessionState().newHadoopConf();
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
    final List<Resource> resources = readResources(source);
    final List<CodeSystem> codeSystems = new ArrayList<>();
    final List<ValueSet> valueSets = new ArrayList<>();
    final List<ConceptMap> conceptMaps = new ArrayList<>();
    for (final Resource resource : resources) {
      if (resource instanceof final CodeSystem codeSystem) {
        codeSystems.add(codeSystem);
      } else if (resource instanceof final ValueSet valueSet) {
        valueSets.add(valueSet);
      } else if (resource instanceof final ConceptMap conceptMap) {
        conceptMaps.add(conceptMap);
      }
    }
    if (codeSystems.isEmpty() && valueSets.isEmpty() && conceptMaps.isEmpty()) {
      throw new TerminologyImportException(
          "No importable FHIR CodeSystem, ValueSet, or ConceptMap resources were found in "
              + source
              + ".");
    }
    // Validate before writing anything so that an invalid source leaves the store untouched.
    codeSystems.forEach(cs -> requireUrl(cs.getUrl(), "CodeSystem"));
    valueSets.forEach(vs -> requireUrl(vs.getUrl(), "ValueSet"));
    conceptMaps.forEach(cm -> requireUrl(cm.getUrl(), "ConceptMap"));

    final TerminologyStoreWriter writer = new TerminologyStoreWriter(spark, storagePath);
    for (final CodeSystem codeSystem : codeSystems) {
      importCodeSystem(writer, codeSystem, source);
    }
    for (final ValueSet valueSet : valueSets) {
      importResource(
          writer,
          VALUE_SET,
          "value_set",
          valueSet.getUrl(),
          valueSet.getVersion(),
          valueSet,
          source);
    }
    for (final ConceptMap conceptMap : conceptMaps) {
      importResource(
          writer,
          CONCEPT_MAP,
          "concept_map",
          conceptMap.getUrl(),
          conceptMap.getVersion(),
          conceptMap,
          source);
    }
    log.info(
        "FHIR import complete: {} code systems, {} value sets, {} concept maps",
        codeSystems.size(),
        valueSets.size(),
        conceptMaps.size());
  }

  private static void requireUrl(@Nullable final String url, @Nonnull final String resourceType) {
    if (url == null || url.isBlank()) {
      throw new TerminologyImportException(
          "A " + resourceType + " resource is missing its canonical url and cannot be imported.");
    }
  }

  // --- Resource reading. ---

  @Nonnull
  private List<Resource> readResources(@Nonnull final String source) {
    final Path root = new Path(source);
    final List<Resource> resources = new ArrayList<>();
    try {
      final FileSystem fs = root.getFileSystem(hadoopConf);
      if (!fs.exists(root)) {
        throw new TerminologyImportException("FHIR source path does not exist: " + source);
      }
      if (fs.getFileStatus(root).isDirectory()) {
        readDirectory(fs, root, resources);
      } else if (isPackage(source)) {
        readPackage(fs, root, resources);
      } else {
        parseInto(readAll(fs, root), source, resources);
      }
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to read the FHIR source at " + source, e);
    }
    return resources;
  }

  private void readDirectory(
      @Nonnull final FileSystem fs,
      @Nonnull final Path root,
      @Nonnull final List<Resource> resources)
      throws IOException {
    final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(root, true);
    while (iterator.hasNext()) {
      final LocatedFileStatus status = iterator.next();
      final String name = status.getPath().getName();
      if (name.endsWith(".json") && !isPackageMetadata(name)) {
        parseInto(readAll(fs, status.getPath()), status.getPath().toString(), resources);
      }
    }
  }

  private void readPackage(
      @Nonnull final FileSystem fs,
      @Nonnull final Path root,
      @Nonnull final List<Resource> resources)
      throws IOException {
    try (TarArchiveInputStream tar =
        new TarArchiveInputStream(new GzipCompressorInputStream(fs.open(root)))) {
      TarArchiveEntry entry;
      while ((entry = tar.getNextEntry()) != null) {
        if (entry.isDirectory()) {
          continue;
        }
        final String name = new Path(entry.getName()).getName();
        if (name.endsWith(".json") && !isPackageMetadata(name)) {
          parseInto(
              new String(IOUtils.toByteArray(tar), StandardCharsets.UTF_8),
              entry.getName(),
              resources);
        }
      }
    }
  }

  /** Excludes the package manifest and index, which are not FHIR resources. */
  private static boolean isPackageMetadata(@Nonnull final String name) {
    return name.equals("package.json") || name.startsWith(".");
  }

  private static boolean isPackage(@Nonnull final String source) {
    final String lower = source.toLowerCase();
    return lower.endsWith(".tgz") || lower.endsWith(".tar.gz");
  }

  @Nonnull
  private static String readAll(@Nonnull final FileSystem fs, @Nonnull final Path path)
      throws IOException {
    try (InputStream in = fs.open(path)) {
      return new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8);
    }
  }

  private static void parseInto(
      @Nonnull final String json,
      @Nonnull final String provenance,
      @Nonnull final List<Resource> resources) {
    final IBaseResource parsed;
    try {
      parsed = parser().parseResource(json);
    } catch (final DataFormatException e) {
      throw new TerminologyImportException(
          "Unable to parse FHIR resource from " + provenance + ": " + e.getMessage(), e);
    }
    if (parsed instanceof final Bundle bundle) {
      for (final Bundle.BundleEntryComponent bundleEntry : bundle.getEntry()) {
        if (bundleEntry.getResource() != null) {
          resources.add(bundleEntry.getResource());
        }
      }
    } else if (parsed instanceof final Resource resource) {
      resources.add(resource);
    }
  }

  // --- CodeSystem flattening. ---

  private void importCodeSystem(
      @Nonnull final TerminologyStoreWriter writer,
      @Nonnull final CodeSystem codeSystem,
      @Nonnull final String source) {
    final String url = codeSystem.getUrl();
    final String version = codeSystem.getVersion();
    final String systemVersionId =
        TerminologyStoreSchema.systemVersionId(url, version == null ? "" : version);

    final Flattened flattened = new Flattened();
    flatten(codeSystem.getConcept(), null, flattened);
    flattened.resolveCodingProperties();

    log.info("Importing FHIR CodeSystem {} ({} concepts)", url, flattened.concepts.size());
    final Dataset<Row> isaEdges =
        withSystemVersion(dataFrame(flattened.isaEdges, isaSchema()), systemVersionId);
    writer.writePartitionedBySystemVersion(
        codeSystemRow(systemVersionId, url, version, codeSystem, flattened.concepts.size()),
        CODE_SYSTEM,
        systemVersionId);
    writer.writePartitionedBySystemVersion(
        withSystemVersion(dataFrame(flattened.concepts, conceptSchema()), systemVersionId),
        CONCEPT,
        systemVersionId);
    writer.writePartitionedBySystemVersion(
        withSystemVersion(dataFrame(flattened.descriptions, descriptionSchema()), systemVersionId),
        DESCRIPTION,
        systemVersionId);
    writer.writePartitionedBySystemVersion(
        withSystemVersion(dataFrame(flattened.properties, propertySchema()), systemVersionId),
        PROPERTY,
        systemVersionId);
    writer.writePartitionedBySystemVersion(
        withSystemVersion(
            dataFrame(flattened.relationships, relationshipSchema()), systemVersionId),
        RELATIONSHIP,
        systemVersionId);
    writer.writePartitionedBySystemVersion(
        new TransitiveClosureBuilder().build(isaEdges), CLOSURE, systemVersionId);
    writer.upsertManifestEntry(
        new ManifestEntry(
            TerminologyStoreSchema.STORE_FORMAT_VERSION,
            "code_system",
            url,
            version,
            source,
            Instant.now()));
  }

  /** Depth-first flattening of the nested concept hierarchy, assigning dense identifiers. */
  private void flatten(
      @Nonnull final List<ConceptDefinitionComponent> concepts,
      @Nullable final Integer parentDense,
      @Nonnull final Flattened out) {
    for (final ConceptDefinitionComponent concept : concepts) {
      final int dense = out.concepts.size();
      out.codeToDense.put(concept.getCode(), dense);
      final boolean active = !isInactive(concept);
      out.concepts.add(
          RowFactory.create(
              concept.getCode(),
              dense,
              active,
              null,
              null,
              false,
              concept.hasDisplay() ? concept.getDisplay() : concept.getCode()));
      for (final ConceptDefinitionDesignationComponent designation : concept.getDesignation()) {
        final Coding use = designation.getUse();
        out.descriptions.add(
            RowFactory.create(
                dense,
                designation.getValue(),
                designation.hasLanguage() ? designation.getLanguage() : null,
                use != null && use.hasCode() ? use.getCode() : null,
                use != null && use.hasSystem() ? use.getSystem() : DESIGNATION_USAGE_SYSTEM,
                null));
      }
      for (final ConceptPropertyComponent property : concept.getProperty()) {
        addProperty(dense, property, out);
      }
      if (parentDense != null) {
        // A nested concept is-a its enclosing concept.
        out.isaEdges.add(RowFactory.create(dense, parentDense));
      }
      flatten(concept.getConcept(), dense, out);
    }
  }

  private void addProperty(
      final int dense,
      @Nonnull final ConceptPropertyComponent property,
      @Nonnull final Flattened out) {
    final Type value = property.getValue();
    if (value == null) {
      return;
    }
    if (value instanceof final Coding coding) {
      // A Coding-valued property becomes a relationship edge when it points within this code
      // system; the target dense identifier is resolved once every concept is known.
      out.codingProperties.add(new CodingProperty(dense, property.getCode(), coding.getCode()));
    } else {
      out.properties.add(
          RowFactory.create(dense, property.getCode(), value.fhirType(), value.primitiveValue()));
    }
  }

  private static boolean isInactive(@Nonnull final ConceptDefinitionComponent concept) {
    return concept.getProperty().stream()
        .anyMatch(
            p ->
                "inactive".equals(p.getCode())
                    && p.getValue() != null
                    && "true".equalsIgnoreCase(p.getValue().primitiveValue()));
  }

  // --- Resource storage. ---

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

  // --- Table construction. ---

  @Nonnull
  private Dataset<Row> dataFrame(@Nonnull final List<Row> rows, @Nonnull final StructType schema) {
    return spark.createDataFrame(rows, schema);
  }

  @Nonnull
  private static Dataset<Row> withSystemVersion(
      @Nonnull final Dataset<Row> data, @Nonnull final String systemVersionId) {
    return data.withColumn(COLUMN_SYSTEM_VERSION_ID, lit(systemVersionId));
  }

  @Nonnull
  private Dataset<Row> codeSystemRow(
      @Nonnull final String systemVersionId,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final CodeSystem codeSystem,
      final long conceptCount) {
    final StructType schema =
        new StructType()
            .add(COLUMN_SYSTEM_VERSION_ID, DataTypes.StringType, false)
            .add(COLUMN_URL, DataTypes.StringType, false)
            .add(COLUMN_VERSION, DataTypes.StringType, true)
            .add(COLUMN_SNOMED_EDITION, DataTypes.StringType, true)
            .add(COLUMN_SNOMED_EFFECTIVE_TIME, DataTypes.StringType, true)
            .add(COLUMN_CONCEPT_COUNT, DataTypes.LongType, false)
            .add(COLUMN_HIERARCHY_MEANING, DataTypes.StringType, true);
    final String hierarchyMeaning =
        codeSystem.hasHierarchyMeaning() ? codeSystem.getHierarchyMeaning().toCode() : "is-a";
    final Row row =
        RowFactory.create(
            systemVersionId, url, version, null, null, conceptCount, hierarchyMeaning);
    return spark.createDataFrame(List.of(row), schema);
  }

  @Nonnull
  private static StructType conceptSchema() {
    return new StructType()
        .add(COLUMN_CODE, DataTypes.StringType, false)
        .add(COLUMN_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_ACTIVE, DataTypes.BooleanType, false)
        .add(COLUMN_EFFECTIVE_TIME, DataTypes.StringType, true)
        .add(COLUMN_MODULE_ID, DataTypes.StringType, true)
        .add(COLUMN_DEFINED, DataTypes.BooleanType, false)
        .add(COLUMN_DISPLAY, DataTypes.StringType, true);
  }

  @Nonnull
  private static StructType descriptionSchema() {
    return new StructType()
        .add(COLUMN_CONCEPT_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_TERM, DataTypes.StringType, true)
        .add(COLUMN_LANGUAGE, DataTypes.StringType, true)
        .add(COLUMN_TYPE_CODE, DataTypes.StringType, true)
        .add(COLUMN_TYPE_SYSTEM, DataTypes.StringType, true)
        .add(
            COLUMN_ACCEPTABILITY,
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
            true);
  }

  @Nonnull
  private static StructType propertySchema() {
    return new StructType()
        .add(COLUMN_CONCEPT_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_PROPERTY_CODE, DataTypes.StringType, false)
        .add(COLUMN_VALUE_TYPE, DataTypes.StringType, false)
        .add(COLUMN_VALUE, DataTypes.StringType, false);
  }

  @Nonnull
  private static StructType relationshipSchema() {
    return new StructType()
        .add(COLUMN_SOURCE_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_TYPE_CODE, DataTypes.StringType, false)
        .add(COLUMN_TARGET_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_ROLE_GROUP, DataTypes.IntegerType, true);
  }

  @Nonnull
  private static StructType isaSchema() {
    return new StructType()
        .add(COLUMN_SOURCE_DENSE_ID, DataTypes.IntegerType, false)
        .add(COLUMN_TARGET_DENSE_ID, DataTypes.IntegerType, false);
  }

  /** The rows accumulated while flattening a single CodeSystem. */
  private static final class Flattened {
    final List<Row> concepts = new ArrayList<>();
    final List<Row> descriptions = new ArrayList<>();
    final List<Row> properties = new ArrayList<>();
    final List<Row> relationships = new ArrayList<>();
    final List<Row> isaEdges = new ArrayList<>();
    final List<CodingProperty> codingProperties = new ArrayList<>();
    final Map<String, Integer> codeToDense = new HashMap<>();

    /** Resolves Coding-valued properties to relationship edges once every concept is known. */
    void resolveCodingProperties() {
      for (final CodingProperty property : codingProperties) {
        final Integer target = codeToDense.get(property.targetCode);
        if (target != null) {
          relationships.add(
              RowFactory.create(property.sourceDense, property.propertyCode, target, null));
        }
      }
    }
  }

  /** A Coding-valued property awaiting resolution of its target to a dense identifier. */
  private static final class CodingProperty {
    final int sourceDense;
    @Nonnull final String propertyCode;
    @Nonnull final String targetCode;

    CodingProperty(
        final int sourceDense,
        @Nonnull final String propertyCode,
        @Nonnull final String targetCode) {
      this.sourceDense = sourceDense;
      this.propertyCode = propertyCode;
      this.targetCode = targetCode;
    }
  }
}
