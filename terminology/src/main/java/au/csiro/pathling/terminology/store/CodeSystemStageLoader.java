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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_PROPERTY_CODE;
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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VALUE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VALUE_TYPE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.DESCRIPTION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.PROPERTY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.RELATIONSHIP;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.when;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

/**
 * Reads the staging files of the CodeSystems of an import through Spark and writes the store
 * tables, each once for every CodeSystem staged. Every staging row carries the system version of
 * its CodeSystem, and every resolution step is keyed by it, so CodeSystems staged together never
 * mix. Duplicate concept codes are resolved to their first occurrence, Coding-valued properties and
 * property-derived hierarchy edges are resolved to dense identifiers by joining on the surviving
 * concepts, and the transitive closure is computed from the combined is-a edges. The manifest is
 * left to the caller, which writes it last so a failure mid-write leaves identifiable, repairable
 * partial versions.
 *
 * @author John Grimes
 */
@Slf4j
public class CodeSystemStageLoader {

  private static final String IS_A = "is-a";

  // The surviving concepts are joined under these names so that neither side of a join shares a
  // column name with the other.
  private static final String SURVIVOR_SYSTEM_VERSION = "survivor_system_version_id";
  private static final String SURVIVOR_CODE = "survivor_code";
  private static final String SURVIVOR_DENSE_ID = "survivor_dense_id";

  @Nonnull private final SparkSession spark;
  @Nonnull private final TerminologyStoreWriter writer;

  /**
   * Creates a stage loader.
   *
   * @param spark the Spark session used to read staging and write the store
   * @param writer the store writer
   */
  public CodeSystemStageLoader(
      @Nonnull final SparkSession spark, @Nonnull final TerminologyStoreWriter writer) {
    this.spark = spark;
    this.writer = writer;
  }

  /**
   * A CodeSystem staged for loading.
   *
   * @param url the CodeSystem canonical URL
   * @param version the CodeSystem version, or null
   * @param hierarchyMeaning the CodeSystem hierarchy meaning, or null (defaults to {@code is-a})
   */
  public record StagedCodeSystem(
      @Nonnull String url, @Nullable String version, @Nullable String hierarchyMeaning) {

    /**
     * Returns the system version identifier that keys this CodeSystem's rows.
     *
     * @return the system version identifier
     */
    @Nonnull
    public String systemVersionId() {
      return TerminologyStoreSchema.systemVersionId(url, version == null ? "" : version);
    }
  }

  /**
   * Loads the staged CodeSystems into the store, writing each content table once. The rows of each
   * CodeSystem replace any stored rows of the same system version.
   *
   * @param staging the sealed staging holding the rows of every CodeSystem in {@code codeSystems}
   * @param codeSystems the staged CodeSystems, each with a distinct system version, not empty
   */
  public void load(
      @Nonnull final CodeSystemStaging staging, @Nonnull final List<StagedCodeSystem> codeSystems) {
    final Map<String, StagedCodeSystem> bySystemVersion = new LinkedHashMap<>();
    for (final StagedCodeSystem codeSystem : codeSystems) {
      bySystemVersion.put(codeSystem.systemVersionId(), codeSystem);
    }
    final List<String> systemVersionIds = List.copyOf(bySystemVersion.keySet());

    // Duplicate resolution: the minimum dense identifier per code survives (first occurrence).
    final Dataset<Row> rawConcepts =
        spark.read().schema(CodeSystemStaging.conceptSchema()).json(staging.conceptPath());
    final Dataset<Row> minByCode =
        rawConcepts
            .groupBy(
                col(COLUMN_SYSTEM_VERSION_ID).alias("dup_system_version_id"),
                col(COLUMN_CODE).alias("dup_code"))
            .agg(min(COLUMN_DENSE_ID).alias("dup_dense"));
    final Dataset<Row> survivors =
        rawConcepts
            .join(
                minByCode,
                col(COLUMN_SYSTEM_VERSION_ID)
                    .equalTo(col("dup_system_version_id"))
                    .and(col(COLUMN_CODE).equalTo(col("dup_code")))
                    .and(col(COLUMN_DENSE_ID).equalTo(col("dup_dense"))))
            .select(
                col(COLUMN_SYSTEM_VERSION_ID),
                col(COLUMN_CODE),
                col(COLUMN_DENSE_ID),
                col(COLUMN_ACTIVE),
                col(COLUMN_DEFINED),
                col(COLUMN_DISPLAY))
            .persist(StorageLevel.DISK_ONLY());
    final Map<String, Long> survivingBySystem = countBySystemVersion(survivors);
    warnOfDropped(
        countBySystemVersion(rawConcepts),
        survivingBySystem,
        bySystemVersion,
        "Dropped {} duplicate concept code(s) in CodeSystem {}, keeping the first occurrence");

    final Dataset<Row> survivorKeys =
        survivors.select(
            col(COLUMN_SYSTEM_VERSION_ID).alias(SURVIVOR_SYSTEM_VERSION),
            col(COLUMN_CODE).alias(SURVIVOR_CODE),
            col(COLUMN_DENSE_ID).alias(SURVIVOR_DENSE_ID));

    final Dataset<Row> relationships =
        resolveCodingProperties(staging, survivorKeys, bySystemVersion);
    final Dataset<Row> isaEdges = resolveIsaEdges(staging, survivorKeys, bySystemVersion);

    // Everything parsed; write the content tables. The caller writes the manifest last.
    log.info(
        "Loading {} CodeSystem(s) ({} concepts) into the store",
        systemVersionIds.size(),
        survivingBySystem.values().stream().mapToLong(Long::longValue).sum());
    write(codeSystemRows(bySystemVersion, survivingBySystem), CODE_SYSTEM, systemVersionIds);
    write(conceptTable(survivors), CONCEPT, systemVersionIds);
    write(descriptionTable(staging, survivorKeys), DESCRIPTION, systemVersionIds);
    write(propertyTable(staging, survivorKeys), PROPERTY, systemVersionIds);
    write(relationships, RELATIONSHIP, systemVersionIds);
    log.info("Computing the transitive closure for {} CodeSystem(s)", systemVersionIds.size());
    final Dataset<Row> closure = new TransitiveClosureBuilder().build(isaEdges);
    write(closure, CLOSURE, systemVersionIds);
    closure.unpersist();
    survivors.unpersist();
    relationships.unpersist();
  }

  /** Writes a content table, replacing the partitions of the CodeSystems being loaded. */
  private void write(
      @Nonnull final Dataset<Row> data,
      @Nonnull final String tableName,
      @Nonnull final List<String> systemVersionIds) {
    writer.writePartitions(data, tableName, COLUMN_SYSTEM_VERSION_ID, systemVersionIds);
  }

  /**
   * Resolves Coding-valued properties to relationship edges by joining the target code to a
   * surviving concept of the same CodeSystem, dropping and counting unmatched (dangling) targets.
   */
  @Nonnull
  private Dataset<Row> resolveCodingProperties(
      @Nonnull final CodeSystemStaging staging,
      @Nonnull final Dataset<Row> survivorKeys,
      @Nonnull final Map<String, StagedCodeSystem> bySystemVersion) {
    final Dataset<Row> codingProperties =
        spark
            .read()
            .schema(CodeSystemStaging.codingPropertySchema())
            .json(staging.codingPropertyPath())
            .join(survivorKeys, survivorByDenseId(COLUMN_SOURCE_DENSE_ID), "left_semi")
            .persist(StorageLevel.DISK_ONLY());
    final Dataset<Row> resolved =
        codingProperties
            .join(survivorKeys, survivorByCode(COLUMN_TARGET_CODE))
            .select(
                col(COLUMN_SOURCE_DENSE_ID),
                col(COLUMN_PROPERTY_CODE).alias(COLUMN_TYPE_CODE),
                col(SURVIVOR_DENSE_ID).alias(COLUMN_TARGET_DENSE_ID),
                lit(null).cast(DataTypes.IntegerType).alias(COLUMN_ROLE_GROUP),
                col(COLUMN_SYSTEM_VERSION_ID))
            .persist(StorageLevel.DISK_ONLY());
    warnOfDropped(
        countBySystemVersion(codingProperties),
        countBySystemVersion(resolved),
        bySystemVersion,
        "Dropped {} Coding-valued property reference(s) with no matching concept in CodeSystem {}");
    codingProperties.unpersist();
    return resolved;
  }

  /**
   * Resolves the is-a edge set for the closure, filtered to surviving concepts on both ends and
   * carrying the system-version key. Nesting-derived edges are used directly; property-derived
   * edges are resolved by joining their referenced code to a surviving concept.
   */
  @Nonnull
  private Dataset<Row> resolveIsaEdges(
      @Nonnull final CodeSystemStaging staging,
      @Nonnull final Dataset<Row> survivorKeys,
      @Nonnull final Map<String, StagedCodeSystem> bySystemVersion) {
    final Dataset<Row> denseEdges =
        spark
            .read()
            .schema(CodeSystemStaging.denseEdgeSchema())
            .json(staging.denseEdgePath())
            .join(survivorKeys, survivorByDenseId(COLUMN_SOURCE_DENSE_ID), "left_semi")
            .join(survivorKeys, survivorByDenseId(COLUMN_TARGET_DENSE_ID), "left_semi")
            .select(
                col(COLUMN_SYSTEM_VERSION_ID),
                col(COLUMN_SOURCE_DENSE_ID),
                col(COLUMN_TARGET_DENSE_ID));
    return denseEdges.union(propertyEdges(staging, survivorKeys, bySystemVersion)).distinct();
  }

  /**
   * Resolves property-derived is-a edges (from {@code parent}/{@code child} concept properties) to
   * dense (child, parent) pairs, dropping and counting dangling references.
   *
   * @param staging the sealed staging
   * @param survivorKeys the surviving concepts of every staged CodeSystem
   * @param bySystemVersion the staged CodeSystems by system version, for the dangling-edge warning
   * @return the resolved property-derived edges as (system_version_id, source_dense_id,
   *     target_dense_id)
   */
  @Nonnull
  private Dataset<Row> propertyEdges(
      @Nonnull final CodeSystemStaging staging,
      @Nonnull final Dataset<Row> survivorKeys,
      @Nonnull final Map<String, StagedCodeSystem> bySystemVersion) {
    // The known side must belong to a surviving concept; edges owned by a dropped duplicate go too.
    final Dataset<Row> codeEdges =
        spark
            .read()
            .schema(CodeSystemStaging.codeEdgeSchema())
            .json(staging.codeEdgePath())
            .join(
                survivorKeys,
                survivorByDenseId(CodeSystemStaging.COLUMN_KNOWN_DENSE_ID),
                "left_semi");
    final Dataset<Row> resolved =
        codeEdges.join(survivorKeys, survivorByCode(CodeSystemStaging.COLUMN_OTHER_CODE));
    warnOfDropped(
        countBySystemVersion(codeEdges),
        countBySystemVersion(resolved),
        bySystemVersion,
        "Dropped {} dangling parent/child reference(s) in CodeSystem {}");
    // Orient each edge: the child is the known side for a parent property, the other side for a
    // child property.
    final Column knownIsChild =
        col(CodeSystemStaging.COLUMN_KNOWN_ROLE).equalTo(lit(CodeSystemStaging.ROLE_CHILD));
    return resolved.select(
        col(COLUMN_SYSTEM_VERSION_ID),
        when(knownIsChild, col(CodeSystemStaging.COLUMN_KNOWN_DENSE_ID))
            .otherwise(col(SURVIVOR_DENSE_ID))
            .alias(COLUMN_SOURCE_DENSE_ID),
        when(knownIsChild, col(SURVIVOR_DENSE_ID))
            .otherwise(col(CodeSystemStaging.COLUMN_KNOWN_DENSE_ID))
            .alias(COLUMN_TARGET_DENSE_ID));
  }

  @Nonnull
  private static Dataset<Row> conceptTable(@Nonnull final Dataset<Row> survivors) {
    return survivors.select(
        col(COLUMN_CODE),
        col(COLUMN_DENSE_ID),
        col(COLUMN_ACTIVE),
        lit(null).cast(DataTypes.StringType).alias(COLUMN_EFFECTIVE_TIME),
        lit(null).cast(DataTypes.StringType).alias(COLUMN_MODULE_ID),
        col(COLUMN_DEFINED),
        col(COLUMN_DISPLAY),
        col(COLUMN_SYSTEM_VERSION_ID));
  }

  @Nonnull
  private Dataset<Row> descriptionTable(
      @Nonnull final CodeSystemStaging staging, @Nonnull final Dataset<Row> survivorKeys) {
    return spark
        .read()
        .schema(CodeSystemStaging.descriptionSchema())
        .json(staging.descriptionPath())
        .join(survivorKeys, survivorByDenseId(COLUMN_CONCEPT_DENSE_ID), "left_semi")
        .select(
            col(COLUMN_CONCEPT_DENSE_ID),
            col(COLUMN_TERM),
            col(COLUMN_LANGUAGE),
            col(COLUMN_TYPE_CODE),
            col(COLUMN_TYPE_SYSTEM),
            lit(null)
                .cast(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
                .alias(COLUMN_ACCEPTABILITY),
            col(COLUMN_SYSTEM_VERSION_ID));
  }

  @Nonnull
  private Dataset<Row> propertyTable(
      @Nonnull final CodeSystemStaging staging, @Nonnull final Dataset<Row> survivorKeys) {
    return spark
        .read()
        .schema(CodeSystemStaging.propertySchema())
        .json(staging.propertyPath())
        .join(survivorKeys, survivorByDenseId(COLUMN_CONCEPT_DENSE_ID), "left_semi")
        .select(
            col(COLUMN_CONCEPT_DENSE_ID),
            col(COLUMN_PROPERTY_CODE),
            col(COLUMN_VALUE_TYPE),
            col(COLUMN_VALUE),
            col(COLUMN_SYSTEM_VERSION_ID));
  }

  /** Builds one code system row per staged CodeSystem, carrying its surviving concept count. */
  @Nonnull
  private Dataset<Row> codeSystemRows(
      @Nonnull final Map<String, StagedCodeSystem> bySystemVersion,
      @Nonnull final Map<String, Long> survivingBySystem) {
    final StructType schema =
        new StructType()
            .add(COLUMN_SYSTEM_VERSION_ID, DataTypes.StringType, false)
            .add(COLUMN_URL, DataTypes.StringType, false)
            .add(COLUMN_VERSION, DataTypes.StringType, true)
            .add(COLUMN_SNOMED_EDITION, DataTypes.StringType, true)
            .add(COLUMN_SNOMED_EFFECTIVE_TIME, DataTypes.StringType, true)
            .add(COLUMN_CONCEPT_COUNT, DataTypes.LongType, false)
            .add(COLUMN_HIERARCHY_MEANING, DataTypes.StringType, true);
    final List<Row> rows =
        bySystemVersion.entrySet().stream()
            .map(
                entry -> {
                  final StagedCodeSystem codeSystem = entry.getValue();
                  return RowFactory.create(
                      entry.getKey(),
                      codeSystem.url(),
                      codeSystem.version(),
                      null,
                      null,
                      survivingBySystem.getOrDefault(entry.getKey(), 0L),
                      codeSystem.hierarchyMeaning() == null ? IS_A : codeSystem.hierarchyMeaning());
                })
            .toList();
    return spark.createDataFrame(rows, schema);
  }

  /** Matches a staging row's dense identifier to a surviving concept of the same CodeSystem. */
  @Nonnull
  private static Column survivorByDenseId(@Nonnull final String denseIdColumn) {
    return col(COLUMN_SYSTEM_VERSION_ID)
        .equalTo(col(SURVIVOR_SYSTEM_VERSION))
        .and(col(denseIdColumn).equalTo(col(SURVIVOR_DENSE_ID)));
  }

  /** Matches a staging row's referenced code to a surviving concept of the same CodeSystem. */
  @Nonnull
  private static Column survivorByCode(@Nonnull final String codeColumn) {
    return col(COLUMN_SYSTEM_VERSION_ID)
        .equalTo(col(SURVIVOR_SYSTEM_VERSION))
        .and(col(codeColumn).equalTo(col(SURVIVOR_CODE)));
  }

  /** Counts the rows of a dataset per system version. */
  @Nonnull
  private static Map<String, Long> countBySystemVersion(@Nonnull final Dataset<Row> data) {
    final Map<String, Long> counts = new HashMap<>();
    for (final Row row : data.groupBy(col(COLUMN_SYSTEM_VERSION_ID)).count().collectAsList()) {
      counts.put(row.getString(0), row.getLong(1));
    }
    return counts;
  }

  /** Logs a warning for each CodeSystem that kept fewer rows than it staged. */
  private static void warnOfDropped(
      @Nonnull final Map<String, Long> staged,
      @Nonnull final Map<String, Long> kept,
      @Nonnull final Map<String, StagedCodeSystem> bySystemVersion,
      @Nonnull final String message) {
    staged.forEach(
        (systemVersionId, count) -> {
          final long dropped = count - kept.getOrDefault(systemVersionId, 0L);
          if (dropped > 0) {
            log.warn(message, dropped, bySystemVersion.get(systemVersionId).url());
          }
        });
  }
}
