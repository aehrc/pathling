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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

/**
 * Reads a CodeSystem's staging files through Spark and writes the store tables. Duplicate concept
 * codes are resolved to their first occurrence, Coding-valued properties and property-derived
 * hierarchy edges are resolved to dense identifiers by joining on the surviving concepts, and the
 * transitive closure is computed from the combined is-a edges. All writes carry the system-version
 * key, and the manifest entry is written last so a failure mid-write leaves an identifiable,
 * repairable partial version.
 *
 * @author John Grimes
 */
@Slf4j
public class CodeSystemStageLoader {

  private static final String IS_A = "is-a";

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
   * Loads a flattened CodeSystem's staging into the store.
   *
   * @param staging the sealed staging holding the CodeSystem's rows
   * @param url the CodeSystem canonical URL
   * @param version the CodeSystem version, or null
   * @param hierarchyMeaning the CodeSystem hierarchy meaning, or null (defaults to {@code is-a})
   * @param source the source path recorded in the manifest for provenance
   */
  public void load(
      @Nonnull final CodeSystemStaging staging,
      @Nonnull final String url,
      @Nullable final String version,
      @Nullable final String hierarchyMeaning,
      @Nonnull final String source) {
    final String systemVersionId =
        TerminologyStoreSchema.systemVersionId(url, version == null ? "" : version);

    // Duplicate resolution: the minimum dense identifier per code survives (first occurrence).
    final Dataset<Row> rawConcepts =
        spark.read().schema(CodeSystemStaging.conceptSchema()).json(staging.conceptPath());
    final Dataset<Row> minByCode =
        rawConcepts
            .groupBy(col(COLUMN_CODE).alias("dup_code"))
            .agg(min(COLUMN_DENSE_ID).alias("dup_dense"));
    final Dataset<Row> survivors =
        rawConcepts
            .join(
                minByCode,
                col(COLUMN_CODE)
                    .equalTo(col("dup_code"))
                    .and(col(COLUMN_DENSE_ID).equalTo(col("dup_dense"))))
            .select(
                col(COLUMN_CODE),
                col(COLUMN_DENSE_ID),
                col(COLUMN_ACTIVE),
                col(COLUMN_DEFINED),
                col(COLUMN_DISPLAY))
            .persist(StorageLevel.MEMORY_AND_DISK());
    final long totalConcepts = rawConcepts.count();
    final long survivingConcepts = survivors.count();
    if (totalConcepts > survivingConcepts) {
      log.warn(
          "Dropped {} duplicate concept code(s) in CodeSystem {}, keeping the first occurrence",
          totalConcepts - survivingConcepts,
          url);
    }

    final Dataset<Row> survivingDense = survivors.select(col(COLUMN_DENSE_ID));
    final Dataset<Row> codeToDense = survivors.select(col(COLUMN_CODE), col(COLUMN_DENSE_ID));

    final Dataset<Row> relationships =
        resolveCodingProperties(staging, survivingDense, codeToDense);
    final Dataset<Row> isaEdges =
        resolveIsaEdges(staging, survivingDense, codeToDense, systemVersionId, url);

    // Everything parsed; write the content tables, then the manifest last.
    log.info("Loading CodeSystem {} ({} concepts) into the store", url, survivingConcepts);
    writer.writePartitionedBySystemVersion(
        codeSystemRow(systemVersionId, url, version, hierarchyMeaning, survivingConcepts),
        CODE_SYSTEM,
        systemVersionId);
    writer.writePartitionedBySystemVersion(
        conceptTable(survivors, systemVersionId), CONCEPT, systemVersionId);
    writer.writePartitionedBySystemVersion(
        descriptionTable(staging, survivingDense, systemVersionId), DESCRIPTION, systemVersionId);
    writer.writePartitionedBySystemVersion(
        propertyTable(staging, survivingDense, systemVersionId), PROPERTY, systemVersionId);
    writer.writePartitionedBySystemVersion(
        withSystemVersion(relationships, systemVersionId), RELATIONSHIP, systemVersionId);
    log.info("Computing the transitive closure for CodeSystem {}", url);
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
    survivors.unpersist();
  }

  /**
   * Resolves Coding-valued properties to relationship edges by joining the target code to a
   * surviving concept, dropping and counting unmatched (dangling) targets.
   */
  @Nonnull
  private Dataset<Row> resolveCodingProperties(
      @Nonnull final CodeSystemStaging staging,
      @Nonnull final Dataset<Row> survivingDense,
      @Nonnull final Dataset<Row> codeToDense) {
    final Dataset<Row> codingProperties =
        spark
            .read()
            .schema(CodeSystemStaging.codingPropertySchema())
            .json(staging.codingPropertyPath())
            .join(
                survivingDense,
                col(COLUMN_SOURCE_DENSE_ID).equalTo(survivingDense.col(COLUMN_DENSE_ID)),
                "left_semi");
    final Dataset<Row> targetByCode =
        codeToDense.select(
            col(COLUMN_CODE).alias("target_code_join"),
            col(COLUMN_DENSE_ID).alias(COLUMN_TARGET_DENSE_ID));
    return codingProperties
        .join(
            targetByCode, codingProperties.col(COLUMN_TARGET_CODE).equalTo(col("target_code_join")))
        .select(
            col(COLUMN_SOURCE_DENSE_ID),
            col(COLUMN_PROPERTY_CODE).alias(COLUMN_TYPE_CODE),
            col(COLUMN_TARGET_DENSE_ID),
            lit(null).cast(DataTypes.IntegerType).alias(COLUMN_ROLE_GROUP));
  }

  /**
   * Resolves the is-a edge set for the closure, filtered to surviving concepts on both ends and
   * carrying the system-version key. Nesting-derived edges are used directly; property-derived
   * edges are resolved by joining their referenced code to a surviving concept.
   */
  @Nonnull
  private Dataset<Row> resolveIsaEdges(
      @Nonnull final CodeSystemStaging staging,
      @Nonnull final Dataset<Row> survivingDense,
      @Nonnull final Dataset<Row> codeToDense,
      @Nonnull final String systemVersionId,
      @Nonnull final String url) {
    final Dataset<Row> denseEdges =
        spark
            .read()
            .schema(CodeSystemStaging.denseEdgeSchema())
            .json(staging.denseEdgePath())
            .join(
                survivingDense.withColumnRenamed(COLUMN_DENSE_ID, "src_survive"),
                col(COLUMN_SOURCE_DENSE_ID).equalTo(col("src_survive")),
                "left_semi")
            .join(
                survivingDense.withColumnRenamed(COLUMN_DENSE_ID, "tgt_survive"),
                col(COLUMN_TARGET_DENSE_ID).equalTo(col("tgt_survive")),
                "left_semi")
            .select(col(COLUMN_SOURCE_DENSE_ID), col(COLUMN_TARGET_DENSE_ID));
    final Dataset<Row> edges =
        denseEdges.union(propertyEdges(staging, codeToDense, url)).distinct();
    return withSystemVersion(edges, systemVersionId);
  }

  /**
   * Resolves property-derived is-a edges (from {@code parent}/{@code child} concept properties) to
   * dense (child, parent) pairs, dropping and counting dangling references.
   *
   * @param staging the sealed staging
   * @param codeToDense the surviving code-to-dense mapping
   * @param url the CodeSystem URL, for the dangling-edge warning
   * @return the resolved property-derived edges as (source_dense_id, target_dense_id)
   */
  @Nonnull
  private Dataset<Row> propertyEdges(
      @Nonnull final CodeSystemStaging staging,
      @Nonnull final Dataset<Row> codeToDense,
      @Nonnull final String url) {
    final Dataset<Row> codeEdges =
        spark.read().schema(CodeSystemStaging.codeEdgeSchema()).json(staging.codeEdgePath());
    final Dataset<Row> otherByCode =
        codeToDense.select(
            col(COLUMN_CODE).alias("other_code_join"), col(COLUMN_DENSE_ID).alias("other_dense"));
    final Dataset<Row> resolved =
        codeEdges.join(
            otherByCode,
            codeEdges.col(CodeSystemStaging.COLUMN_OTHER_CODE).equalTo(col("other_code_join")));
    final long dangling = codeEdges.count() - resolved.count();
    if (dangling > 0) {
      log.warn("Dropped {} dangling parent/child reference(s) in CodeSystem {}", dangling, url);
    }
    // Orient each edge: the child is the known side for a parent property, the other side for a
    // child property.
    return resolved.select(
        org.apache
            .spark
            .sql
            .functions
            .when(
                col(CodeSystemStaging.COLUMN_KNOWN_ROLE).equalTo(lit("child")),
                col(CodeSystemStaging.COLUMN_KNOWN_DENSE_ID))
            .otherwise(col("other_dense"))
            .alias(COLUMN_SOURCE_DENSE_ID),
        org.apache
            .spark
            .sql
            .functions
            .when(
                col(CodeSystemStaging.COLUMN_KNOWN_ROLE).equalTo(lit("child")), col("other_dense"))
            .otherwise(col(CodeSystemStaging.COLUMN_KNOWN_DENSE_ID))
            .alias(COLUMN_TARGET_DENSE_ID));
  }

  @Nonnull
  private Dataset<Row> conceptTable(
      @Nonnull final Dataset<Row> survivors, @Nonnull final String systemVersionId) {
    return survivors.select(
        col(COLUMN_CODE),
        col(COLUMN_DENSE_ID),
        col(COLUMN_ACTIVE),
        lit(null).cast(DataTypes.StringType).alias(COLUMN_EFFECTIVE_TIME),
        lit(null).cast(DataTypes.StringType).alias(COLUMN_MODULE_ID),
        col(COLUMN_DEFINED),
        col(COLUMN_DISPLAY),
        lit(systemVersionId).alias(COLUMN_SYSTEM_VERSION_ID));
  }

  @Nonnull
  private Dataset<Row> descriptionTable(
      @Nonnull final CodeSystemStaging staging,
      @Nonnull final Dataset<Row> survivingDense,
      @Nonnull final String systemVersionId) {
    return spark
        .read()
        .schema(CodeSystemStaging.descriptionSchema())
        .json(staging.descriptionPath())
        .join(
            survivingDense,
            col(COLUMN_CONCEPT_DENSE_ID).equalTo(survivingDense.col(COLUMN_DENSE_ID)),
            "left_semi")
        .select(
            col(COLUMN_CONCEPT_DENSE_ID),
            col(COLUMN_TERM),
            col(COLUMN_LANGUAGE),
            col(COLUMN_TYPE_CODE),
            col(COLUMN_TYPE_SYSTEM),
            lit(null)
                .cast(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
                .alias(COLUMN_ACCEPTABILITY),
            lit(systemVersionId).alias(COLUMN_SYSTEM_VERSION_ID));
  }

  @Nonnull
  private Dataset<Row> propertyTable(
      @Nonnull final CodeSystemStaging staging,
      @Nonnull final Dataset<Row> survivingDense,
      @Nonnull final String systemVersionId) {
    return spark
        .read()
        .schema(CodeSystemStaging.propertySchema())
        .json(staging.propertyPath())
        .join(
            survivingDense,
            col(COLUMN_CONCEPT_DENSE_ID).equalTo(survivingDense.col(COLUMN_DENSE_ID)),
            "left_semi")
        .select(
            col(COLUMN_CONCEPT_DENSE_ID),
            col(COLUMN_PROPERTY_CODE),
            col(COLUMN_VALUE_TYPE),
            col(COLUMN_VALUE),
            lit(systemVersionId).alias(COLUMN_SYSTEM_VERSION_ID));
  }

  @Nonnull
  private Dataset<Row> codeSystemRow(
      @Nonnull final String systemVersionId,
      @Nonnull final String url,
      @Nullable final String version,
      @Nullable final String hierarchyMeaning,
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
            url,
            version,
            null,
            null,
            conceptCount,
            hierarchyMeaning == null ? IS_A : hierarchyMeaning);
    return spark.createDataFrame(List.of(row), schema);
  }

  @Nonnull
  private static Dataset<Row> withSystemVersion(
      @Nonnull final Dataset<Row> data, @Nonnull final String systemVersionId) {
    return data.withColumn(COLUMN_SYSTEM_VERSION_ID, lit(systemVersionId));
  }
}
