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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_REFERENCED_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_REFSET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ROLE_GROUP;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SNOMED_EDITION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SNOMED_EFFECTIVE_TIME;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TERM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TYPE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TYPE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_URL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.DESCRIPTION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.MANIFEST;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.REFSET_MEMBER;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.RELATIONSHIP;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.map_from_entries;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.struct;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Imports a SNOMED CT RF2 snapshot release into the terminology store. The release is read through
 * the Hadoop FileSystem API and parsed with Spark; concepts are assigned dense identifiers, the
 * transitive closure of the is-a hierarchy is precomputed, and the resulting tables are written to
 * the store, replacing any previous content for the same code system version atomically.
 *
 * <p>Only snapshot releases are supported. The edition and version are detected from the release's
 * module and effectiveTime content, or taken from an explicit override. A release whose input is
 * malformed, or that is not a snapshot, is rejected before any table is written, so the store is
 * left unchanged.
 *
 * @author John Grimes
 */
@Slf4j
public class SnomedRf2Importer {

  private static final String SNOMED_URI = "http://snomed.info/sct";
  private static final String IS_A = "116680003";
  private static final String FSN_TYPE = "900000000000003001";
  private static final String SYNONYM_TYPE = "900000000000013009";
  private static final String PREFERRED = "900000000000548007";
  private static final String DEFINED_STATUS = "900000000000073002";
  private static final String TARGET_COMPONENT_ID = "targetComponentId";
  private static final String ACCEPTABILITY_ID = "acceptabilityId";

  private static final Pattern SNOMED_VERSION =
      Pattern.compile("http://snomed.info/x?sct/(?<edition>\\d+)/version/(?<time>\\d{8})");

  @Nonnull private final SparkSession spark;
  @Nonnull private final String storagePath;
  @Nonnull private final Configuration hadoopConf;

  /**
   * Creates an importer targeting a store.
   *
   * @param spark the Spark session used to read and write
   * @param storagePath the root path of the terminology store, created if absent
   */
  public SnomedRf2Importer(@Nonnull final SparkSession spark, @Nonnull final String storagePath) {
    this.spark = spark;
    this.storagePath = storagePath;
    this.hadoopConf = spark.sessionState().newHadoopConf();
  }

  /**
   * Imports an RF2 snapshot release.
   *
   * @param source the release directory or archive, on any Hadoop-accessible filesystem
   * @param editionUriOverride an explicit edition/version URI, or null to detect it
   * @throws TerminologyImportException if the source is not a valid RF2 snapshot release
   */
  public void importFrom(@Nonnull final String source, @Nullable final String editionUriOverride) {
    final Rf2Files files = locateFiles(source);

    log.info("Reading concepts from {}", files.concept);
    final Dataset<Row> conceptRaw = readRf2(files.concept, "id", COLUMN_ACTIVE, "moduleId");
    validateColumns(
        files.concept, conceptRaw, "id", COLUMN_ACTIVE, "moduleId", "definitionStatusId");

    final String version =
        editionUriOverride != null ? editionUriOverride : detectEditionUri(conceptRaw);
    final String systemVersionId = systemVersionId(SNOMED_URI, version);
    final SnomedVersion parsed = parseVersion(version);
    log.info("Detected SNOMED CT edition {} version {}", parsed.edition, version);

    // Concept dictionary with dense identifiers, ordered by code for determinism.
    final Dataset<Row> concepts =
        conceptRaw
            .select(
                col("id").alias(COLUMN_CODE),
                col(COLUMN_ACTIVE).equalTo("1").alias(COLUMN_ACTIVE),
                col("effectiveTime").alias(COLUMN_EFFECTIVE_TIME),
                col("moduleId").alias(COLUMN_MODULE_ID),
                col("definitionStatusId").equalTo(DEFINED_STATUS).alias(COLUMN_DEFINED))
            .withColumn(COLUMN_DENSE_ID, row_number().over(Window.orderBy(COLUMN_CODE)).minus(1))
            .persist();
    final long conceptCount = concepts.count();

    final Dataset<Row> denseByCode = concepts.select(COLUMN_CODE, COLUMN_DENSE_ID);

    // Descriptions and the display term.
    final Descriptions descriptions =
        readDescriptions(files, concepts, denseByCode, systemVersionId);
    final Dataset<Row> conceptTable =
        buildConceptTable(concepts, descriptions.display, systemVersionId);

    // Relationships: is-a feeds the closure, other attributes are stored.
    final Relationships relationships = readRelationships(files, denseByCode, systemVersionId);

    // Reference set membership.
    final Dataset<Row> refsetMembers = readRefsets(files, denseByCode, systemVersionId);

    final Dataset<Row> codeSystemRow =
        codeSystemRow(systemVersionId, version, parsed, conceptCount);

    // All parsing succeeded; write the content tables, then the manifest last.
    log.info("Writing {} concepts to the store", conceptCount);
    final TerminologyStoreWriter writer = new TerminologyStoreWriter(spark, storagePath);
    writePartitioned(writer, codeSystemRow, CODE_SYSTEM, systemVersionId);
    writePartitioned(writer, conceptTable, CONCEPT, systemVersionId);
    writePartitioned(writer, descriptions.table, DESCRIPTION, systemVersionId);
    writePartitioned(writer, relationships.attributes, RELATIONSHIP, systemVersionId);
    log.info("Computing the transitive closure");
    writePartitioned(
        writer, new TransitiveClosureBuilder().build(relationships.isa), CLOSURE, systemVersionId);
    writePartitioned(writer, refsetMembers, REFSET_MEMBER, systemVersionId);
    writeManifest(writer, version, source);
    concepts.unpersist();
    log.info("Import complete for {} version {} ({} concepts)", SNOMED_URI, version, conceptCount);
  }

  // --- File discovery. ---

  /** The RF2 files located within a release's Snapshot directory. */
  private static final class Rf2Files {
    @Nonnull final String concept;
    @Nullable final String description;
    @Nullable final String relationship;
    @Nullable final String language;
    @Nonnull final List<String> otherRefsets;

    Rf2Files(
        @Nonnull final String concept,
        @Nullable final String description,
        @Nullable final String relationship,
        @Nullable final String language,
        @Nonnull final List<String> otherRefsets) {
      this.concept = concept;
      this.description = description;
      this.relationship = relationship;
      this.language = language;
      this.otherRefsets = otherRefsets;
    }
  }

  @Nonnull
  private Rf2Files locateFiles(@Nonnull final String source) {
    final Path root = new Path(source);
    String concept = null;
    String description = null;
    String relationship = null;
    String language = null;
    final List<String> otherRefsets = new ArrayList<>();
    boolean sawNonSnapshotRelease = false;

    try {
      final FileSystem fs = root.getFileSystem(hadoopConf);
      if (!fs.exists(root)) {
        throw new TerminologyImportException("RF2 source path does not exist: " + source);
      }
      final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(root, true);
      while (iterator.hasNext()) {
        final LocatedFileStatus status = iterator.next();
        final String path = status.getPath().toString();
        final String name = status.getPath().getName();
        if (!path.contains("/Snapshot/")) {
          if (path.contains("/Full/") || path.contains("/Delta/")) {
            sawNonSnapshotRelease = true;
          }
          continue;
        }
        if (name.startsWith("sct2_Concept_")) {
          concept = path;
        } else if (name.startsWith("sct2_Description_")
            || name.startsWith("sct2_TextDefinition_")) {
          description = path;
        } else if (name.startsWith("sct2_Relationship_")) {
          relationship = path;
        } else if (name.contains("Refset_Language")) {
          language = path;
        } else if (name.startsWith("der2_") && name.contains("Refset")) {
          otherRefsets.add(path);
        }
      }
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to read the RF2 source at " + source, e);
    }

    if (concept == null) {
      final String detail =
          sawNonSnapshotRelease
              ? " Only snapshot releases are supported; this appears to be a full or delta release."
              : "";
      throw new TerminologyImportException(
          "No SNOMED CT snapshot concept file was found under " + source + "." + detail);
    }
    return new Rf2Files(concept, description, relationship, language, otherRefsets);
  }

  // --- RF2 parsing. ---

  /**
   * Reads an RF2 tab-delimited file into a data frame with a string column per header field,
   * tolerant of the CRLF line endings of real releases.
   */
  @Nonnull
  private Dataset<Row> readRf2(@Nonnull final String path, @Nonnull final String... required) {
    final String[] columns = readHeader(path);
    StructType schema = new StructType();
    for (final String column : columns) {
      schema = schema.add(column, DataTypes.StringType, true);
    }
    final String headerLine = String.join("\t", columns);
    final int arity = columns.length;
    final Dataset<Row> rows =
        spark
            .read()
            .textFile(path)
            .filter((FilterFunction<String>) line -> !line.isEmpty() && !line.equals(headerLine))
            .map(
                (MapFunction<String, Row>)
                    line -> {
                      final String[] parts = line.split("\t", -1);
                      final Object[] values = new Object[arity];
                      for (int i = 0; i < arity; i++) {
                        values[i] = i < parts.length ? parts[i] : null;
                      }
                      return RowFactory.create(values);
                    },
                Encoders.row(schema));
    for (final String column : required) {
      if (schema.getFieldIndex(column).isEmpty()) {
        throw new TerminologyImportException(
            "RF2 file " + path + " is missing the expected column '" + column + "'.");
      }
    }
    return rows;
  }

  @Nonnull
  private String[] readHeader(@Nonnull final String path) {
    final Path file = new Path(path);
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                file.getFileSystem(hadoopConf).open(file), StandardCharsets.UTF_8))) {
      final String header = reader.readLine();
      if (header == null || header.isEmpty()) {
        throw new TerminologyImportException("RF2 file is empty: " + path);
      }
      return header.split("\t", -1);
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to read RF2 file header: " + path, e);
    }
  }

  private void validateColumns(
      @Nonnull final String path,
      @Nonnull final Dataset<Row> data,
      @Nonnull final String... expected) {
    for (final String column : expected) {
      if (data.schema().getFieldIndex(column).isEmpty()) {
        throw new TerminologyImportException(
            "RF2 file " + path + " does not have the expected column '" + column + "'.");
      }
    }
  }

  // --- Descriptions. ---

  /** The description table plus a per-concept display term data frame ({@code code}, display). */
  private static final class Descriptions {
    @Nonnull final Dataset<Row> table;
    @Nonnull final Dataset<Row> display;

    Descriptions(@Nonnull final Dataset<Row> table, @Nonnull final Dataset<Row> display) {
      this.table = table;
      this.display = display;
    }
  }

  @Nonnull
  private Descriptions readDescriptions(
      @Nonnull final Rf2Files files,
      @Nonnull final Dataset<Row> concepts,
      @Nonnull final Dataset<Row> denseByCode,
      @Nonnull final String systemVersionId) {
    if (files.description == null) {
      final Dataset<Row> emptyTable = emptyDescriptionTable();
      final Dataset<Row> emptyDisplay =
          concepts
              .select(col(COLUMN_CODE), lit(null).cast("string").alias(COLUMN_DISPLAY))
              .limit(0);
      return new Descriptions(emptyTable, emptyDisplay);
    }

    final Dataset<Row> descRaw =
        readRf2(files.description, "id", "conceptId", "typeId", COLUMN_TERM)
            .filter(col(COLUMN_ACTIVE).equalTo("1"));

    // Active language reference set rows: description id -> (refset, acceptability).
    final Dataset<Row> langActive =
        files.language == null
            ? emptyLanguage()
            : readRf2(files.language, "referencedComponentId", "refsetId", ACCEPTABILITY_ID)
                .filter(col(COLUMN_ACTIVE).equalTo("1"))
                .select(
                    col("referencedComponentId").alias("descId"),
                    col("refsetId"),
                    col(ACCEPTABILITY_ID));

    // Acceptability map per description.
    final Dataset<Row> acceptability =
        langActive
            .groupBy("descId")
            .agg(
                map_from_entries(collect_list(struct(col("refsetId"), col(ACCEPTABILITY_ID))))
                    .alias(COLUMN_ACCEPTABILITY));

    final Dataset<Row> table =
        descRaw
            .join(denseByCode, descRaw.col("conceptId").equalTo(denseByCode.col(COLUMN_CODE)))
            .join(
                acceptability, descRaw.col("id").equalTo(acceptability.col("descId")), "left_outer")
            .select(
                lit(systemVersionId).alias(COLUMN_SYSTEM_VERSION_ID),
                col(COLUMN_DENSE_ID).alias(COLUMN_CONCEPT_DENSE_ID),
                col(COLUMN_TERM),
                col("languageCode").alias(COLUMN_LANGUAGE),
                col("typeId").alias(COLUMN_TYPE_CODE),
                lit(SNOMED_URI).alias(COLUMN_TYPE_SYSTEM),
                col(COLUMN_ACCEPTABILITY));

    // Preferred synonym per concept, with the FSN and code as fallbacks for the display.
    final Dataset<Row> preferredDescriptionIds =
        langActive.filter(col(ACCEPTABILITY_ID).equalTo(PREFERRED)).select("descId").distinct();
    final Dataset<Row> preferredSynonym =
        descRaw
            .filter(col("typeId").equalTo(SYNONYM_TYPE))
            .join(
                preferredDescriptionIds,
                descRaw.col("id").equalTo(preferredDescriptionIds.col("descId")))
            .groupBy(col("conceptId").alias(COLUMN_CODE))
            .agg(min(COLUMN_TERM).alias("preferredTerm"));
    final Dataset<Row> fsn =
        descRaw
            .filter(col("typeId").equalTo(FSN_TYPE))
            .groupBy(col("conceptId").alias(COLUMN_CODE))
            .agg(min(COLUMN_TERM).alias("fsnTerm"));
    final Dataset<Row> display =
        concepts
            .select(COLUMN_CODE)
            .join(preferredSynonym, "code", "left_outer")
            .join(fsn, "code", "left_outer")
            .select(
                col(COLUMN_CODE),
                coalesce(col("preferredTerm"), col("fsnTerm"), col(COLUMN_CODE))
                    .alias(COLUMN_DISPLAY));

    return new Descriptions(table, display);
  }

  @Nonnull
  private Dataset<Row> buildConceptTable(
      @Nonnull final Dataset<Row> concepts,
      @Nonnull final Dataset<Row> display,
      @Nonnull final String systemVersionId) {
    return concepts
        .join(display, "code", "left_outer")
        .select(
            lit(systemVersionId).alias(COLUMN_SYSTEM_VERSION_ID),
            col(COLUMN_CODE),
            col(COLUMN_DENSE_ID),
            col(COLUMN_ACTIVE),
            col(COLUMN_EFFECTIVE_TIME),
            col(COLUMN_MODULE_ID),
            col(COLUMN_DEFINED),
            coalesce(col(COLUMN_DISPLAY), col(COLUMN_CODE)).alias(COLUMN_DISPLAY));
  }

  // --- Relationships. ---

  /** The is-a edges (for the closure) and the stored attribute relationships. */
  private static final class Relationships {
    @Nonnull final Dataset<Row> isa;
    @Nonnull final Dataset<Row> attributes;

    Relationships(@Nonnull final Dataset<Row> isa, @Nonnull final Dataset<Row> attributes) {
      this.isa = isa;
      this.attributes = attributes;
    }
  }

  @Nonnull
  private Relationships readRelationships(
      @Nonnull final Rf2Files files,
      @Nonnull final Dataset<Row> denseByCode,
      @Nonnull final String systemVersionId) {
    if (files.relationship == null) {
      return new Relationships(emptyIsa(), emptyRelationshipTable());
    }
    final Dataset<Row> relRaw =
        readRf2(files.relationship, "sourceId", "destinationId", "typeId")
            .filter(col(COLUMN_ACTIVE).equalTo("1"));

    final Dataset<Row> sourceDense =
        denseByCode.withColumnRenamed(COLUMN_DENSE_ID, COLUMN_SOURCE_DENSE_ID);
    final Dataset<Row> targetDense =
        denseByCode.withColumnRenamed(COLUMN_DENSE_ID, COLUMN_TARGET_DENSE_ID);

    final Dataset<Row> mapped =
        relRaw
            .join(sourceDense, relRaw.col("sourceId").equalTo(sourceDense.col(COLUMN_CODE)))
            .join(targetDense, relRaw.col("destinationId").equalTo(targetDense.col(COLUMN_CODE)))
            .select(
                col("typeId"),
                col("relationshipGroup"),
                col(COLUMN_SOURCE_DENSE_ID),
                col(COLUMN_TARGET_DENSE_ID))
            .persist();

    final Dataset<Row> isa =
        mapped
            .filter(col("typeId").equalTo(IS_A))
            .select(
                lit(systemVersionId).alias(COLUMN_SYSTEM_VERSION_ID),
                col(COLUMN_SOURCE_DENSE_ID),
                col(COLUMN_TARGET_DENSE_ID));
    final Dataset<Row> attributes =
        mapped
            .filter(col("typeId").notEqual(IS_A))
            .select(
                lit(systemVersionId).alias(COLUMN_SYSTEM_VERSION_ID),
                col(COLUMN_SOURCE_DENSE_ID),
                col("typeId").alias(COLUMN_TYPE_CODE),
                col(COLUMN_TARGET_DENSE_ID),
                col("relationshipGroup").cast(DataTypes.IntegerType).alias(COLUMN_ROLE_GROUP));
    return new Relationships(isa, attributes);
  }

  // --- Reference sets. ---

  @Nonnull
  private Dataset<Row> readRefsets(
      @Nonnull final Rf2Files files,
      @Nonnull final Dataset<Row> denseByCode,
      @Nonnull final String systemVersionId) {
    Dataset<Row> members = null;
    for (final String path : files.otherRefsets) {
      final Dataset<Row> raw =
          readRf2(path, "refsetId", "referencedComponentId")
              .filter(col(COLUMN_ACTIVE).equalTo("1"));
      final boolean hasTarget = raw.schema().getFieldIndex(TARGET_COMPONENT_ID).isDefined();
      final Dataset<Row> targetColumn =
          hasTarget
              ? raw.withColumn(COLUMN_TARGET_CODE, col(TARGET_COMPONENT_ID))
              : raw.withColumn(COLUMN_TARGET_CODE, lit(null).cast(DataTypes.StringType));
      final Dataset<Row> mapped =
          targetColumn
              .join(
                  denseByCode,
                  targetColumn.col("referencedComponentId").equalTo(denseByCode.col(COLUMN_CODE)))
              .select(
                  lit(systemVersionId).alias(COLUMN_SYSTEM_VERSION_ID),
                  col("refsetId").alias(COLUMN_REFSET_CODE),
                  col(COLUMN_DENSE_ID).alias(COLUMN_REFERENCED_DENSE_ID),
                  col(COLUMN_TARGET_CODE));
      members = members == null ? mapped : members.unionByName(mapped);
    }
    return members == null ? emptyRefsetTable(systemVersionId) : members;
  }

  // --- Metadata rows. ---

  @Nonnull
  private Dataset<Row> codeSystemRow(
      @Nonnull final String systemVersionId,
      @Nonnull final String version,
      @Nonnull final SnomedVersion parsed,
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
    final Row row =
        RowFactory.create(
            systemVersionId,
            SNOMED_URI,
            version,
            parsed.edition,
            parsed.effectiveTime,
            conceptCount,
            "is-a");
    return spark.createDataFrame(List.of(row), schema);
  }

  private void writeManifest(
      @Nonnull final TerminologyStoreWriter writer,
      @Nonnull final String version,
      @Nonnull final String source) {
    final ManifestEntry entry =
        new ManifestEntry(
            TerminologyStoreSchema.STORE_FORMAT_VERSION,
            "code_system",
            SNOMED_URI,
            version,
            source,
            Instant.now());
    if (tableExists(MANIFEST)) {
      // Atomically replace only this system's manifest entry, leaving other entries untouched.
      final List<Row> rows =
          List.of(
              RowFactory.create(
                  entry.getStoreFormatVersion(),
                  entry.getEntryType(),
                  entry.getCanonicalUrl(),
                  entry.getVersion(),
                  entry.getSource(),
                  java.sql.Timestamp.from(entry.getImportedAt())));
      final Dataset<Row> data =
          spark.createDataFrame(rows, TerminologyStoreSchema.manifestSchema());
      writer.replaceWhere(
          data, MANIFEST, canonicalUrlPredicate(SNOMED_URI) + " AND " + versionPredicate(version));
    } else {
      writer.writeManifest(List.of(entry), SaveMode.Append);
    }
  }

  @Nonnull
  private static String canonicalUrlPredicate(@Nonnull final String url) {
    return TerminologyStoreSchema.COLUMN_CANONICAL_URL + " = '" + url + "'";
  }

  @Nonnull
  private static String versionPredicate(@Nonnull final String version) {
    return COLUMN_VERSION + " = '" + version + "'";
  }

  // --- Writing helpers. ---

  private void writePartitioned(
      @Nonnull final TerminologyStoreWriter writer,
      @Nonnull final Dataset<Row> data,
      @Nonnull final String tableName,
      @Nonnull final String systemVersionId) {
    if (tableExists(tableName)) {
      writer.replaceWhere(
          data, tableName, COLUMN_SYSTEM_VERSION_ID + " = '" + systemVersionId + "'");
    } else {
      writer.writeTable(data, tableName, SaveMode.Overwrite, List.of(COLUMN_SYSTEM_VERSION_ID));
    }
  }

  private boolean tableExists(@Nonnull final String tableName) {
    final Path log =
        new Path(TerminologyStoreSchema.tablePath(storagePath, tableName), "_delta_log");
    try {
      return log.getFileSystem(hadoopConf).exists(log);
    } catch (final IOException e) {
      return false;
    }
  }

  // --- Version detection. ---

  /** The parsed SNOMED edition module and effectiveTime of a version URI. */
  private static final class SnomedVersion {
    @Nullable final String edition;
    @Nullable final String effectiveTime;

    SnomedVersion(@Nullable final String edition, @Nullable final String effectiveTime) {
      this.edition = edition;
      this.effectiveTime = effectiveTime;
    }
  }

  @Nonnull
  private String detectEditionUri(@Nonnull final Dataset<Row> conceptRaw) {
    final Row row =
        conceptRaw
            .filter(col(COLUMN_ACTIVE).equalTo("1"))
            .groupBy("moduleId")
            .count()
            .orderBy(col("count").desc(), col("moduleId"))
            .first();
    final String module = row.getString(0);
    final String effectiveTime =
        conceptRaw.agg(org.apache.spark.sql.functions.max("effectiveTime")).first().getString(0);
    return SNOMED_URI + "/" + module + "/version/" + effectiveTime;
  }

  @Nonnull
  private SnomedVersion parseVersion(@Nonnull final String version) {
    final Matcher matcher = SNOMED_VERSION.matcher(version);
    if (matcher.find()) {
      return new SnomedVersion(matcher.group("edition"), matcher.group("time"));
    }
    return new SnomedVersion(null, null);
  }

  @Nonnull
  private static String systemVersionId(@Nonnull final String url, @Nonnull final String version) {
    try {
      final MessageDigest digest = MessageDigest.getInstance("SHA-256");
      final byte[] hash = digest.digest((url + "|" + version).getBytes(StandardCharsets.UTF_8));
      final StringBuilder builder = new StringBuilder();
      for (int i = 0; i < 8; i++) {
        builder.append(String.format("%02x", hash[i]));
      }
      return builder.toString();
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

  // --- Empty data frames for absent files. ---

  @Nonnull
  private Dataset<Row> emptyLanguage() {
    final StructType schema =
        new StructType()
            .add("descId", DataTypes.StringType, true)
            .add("refsetId", DataTypes.StringType, true)
            .add(ACCEPTABILITY_ID, DataTypes.StringType, true);
    return spark.createDataFrame(new ArrayList<>(), schema);
  }

  @Nonnull
  private Dataset<Row> emptyIsa() {
    final StructType schema =
        new StructType()
            .add(COLUMN_SYSTEM_VERSION_ID, DataTypes.StringType, false)
            .add(COLUMN_SOURCE_DENSE_ID, DataTypes.IntegerType, false)
            .add(COLUMN_TARGET_DENSE_ID, DataTypes.IntegerType, false);
    return spark.createDataFrame(new ArrayList<>(), schema);
  }

  @Nonnull
  private Dataset<Row> emptyRelationshipTable() {
    final StructType schema =
        new StructType()
            .add(COLUMN_SYSTEM_VERSION_ID, DataTypes.StringType, false)
            .add(COLUMN_SOURCE_DENSE_ID, DataTypes.IntegerType, false)
            .add(COLUMN_TYPE_CODE, DataTypes.StringType, false)
            .add(COLUMN_TARGET_DENSE_ID, DataTypes.IntegerType, false)
            .add(COLUMN_ROLE_GROUP, DataTypes.IntegerType, true);
    return spark.createDataFrame(new ArrayList<>(), schema);
  }

  @Nonnull
  private Dataset<Row> emptyRefsetTable(@Nonnull final String systemVersionId) {
    final StructType schema =
        new StructType()
            .add(COLUMN_SYSTEM_VERSION_ID, DataTypes.StringType, false)
            .add(COLUMN_REFSET_CODE, DataTypes.StringType, false)
            .add(COLUMN_REFERENCED_DENSE_ID, DataTypes.IntegerType, false)
            .add(COLUMN_TARGET_CODE, DataTypes.StringType, true);
    return spark.createDataFrame(new ArrayList<>(), schema);
  }

  @Nonnull
  private Dataset<Row> emptyDescriptionTable() {
    final StructType schema =
        new StructType()
            .add(COLUMN_SYSTEM_VERSION_ID, DataTypes.StringType, false)
            .add(COLUMN_CONCEPT_DENSE_ID, DataTypes.IntegerType, false)
            .add(COLUMN_TERM, DataTypes.StringType, true)
            .add(COLUMN_LANGUAGE, DataTypes.StringType, true)
            .add(COLUMN_TYPE_CODE, DataTypes.StringType, true)
            .add(COLUMN_TYPE_SYSTEM, DataTypes.StringType, true)
            .add(
                COLUMN_ACCEPTABILITY,
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
                true);
    return spark.createDataFrame(new ArrayList<>(), schema);
  }
}
