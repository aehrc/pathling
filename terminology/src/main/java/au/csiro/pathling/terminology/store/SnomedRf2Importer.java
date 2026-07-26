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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.REFSET_MEMBER;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.RELATIONSHIP;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.map_from_entries;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.udf;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Observation;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

/**
 * Imports a SNOMED CT RF2 snapshot release into the terminology store. The release is read through
 * the Hadoop FileSystem API and parsed with Spark; concepts are assigned dense identifiers, the
 * transitive closure of the is-a hierarchy is precomputed, and the resulting tables are written to
 * the store, replacing any previous content for the same code system version atomically.
 *
 * <p>Only snapshot releases are supported. The edition module is detected from the module
 * dependency reference set when present (falling back to the most frequent concept module), the
 * version from the maximum effectiveTime, or both are taken from an explicit override. A release
 * whose input is malformed, or that is not a snapshot, is rejected before any table is written, so
 * the store is left unchanged.
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
  private static final String DESCRIPTION_ID = "descriptionId";

  /** The name of the row count metric carried by every resolution observation. */
  private static final String OBSERVED_ROWS = "rows";

  /**
   * How long to wait, in total, for a batch of observed row counts to arrive before abandoning
   * their lines. Spark delivers observed metrics through the asynchronous listener bus, whose
   * events may be dropped under load, so waiting indefinitely would risk stalling an otherwise
   * complete import for the sake of a diagnostic.
   */
  private static final long METRIC_TIMEOUT_SECONDS = 60;

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
    importFrom(source, editionUriOverride, DenseIdOrder.CODE_ORDER);
  }

  /**
   * Imports an RF2 snapshot release, choosing how dense identifiers are assigned.
   *
   * @param source the release directory or archive, on any Hadoop-accessible filesystem
   * @param editionUriOverride an explicit edition/version URI, or null to detect it
   * @param denseIdOrder the rule for assigning dense identifiers
   * @throws TerminologyImportException if the source is not a valid RF2 snapshot release
   */
  public void importFrom(
      @Nonnull final String source,
      @Nullable final String editionUriOverride,
      @Nonnull final DenseIdOrder denseIdOrder) {
    // A zip archive is extracted to a temporary directory first, since the file discovery and Spark
    // readers operate on the extracted release layout. A plain directory is read in place.
    final java.nio.file.Path extracted = isZipArchive(source) ? extractArchive(source) : null;
    try {
      final String releaseRoot = extracted != null ? extracted.toString() : source;
      importFromRelease(releaseRoot, source, editionUriOverride, denseIdOrder);
    } finally {
      if (extracted != null) {
        deleteRecursively(extracted);
      }
    }
  }

  /**
   * Imports an RF2 snapshot release from an extracted directory layout.
   *
   * @param releaseRoot the directory holding the extracted release, scanned for the Snapshot files
   * @param source the original source path, recorded in the manifest for provenance
   * @param editionUriOverride an explicit edition/version URI, or null to detect it
   * @param denseIdOrder the rule for assigning dense identifiers
   */
  private void importFromRelease(
      @Nonnull final String releaseRoot,
      @Nonnull final String source,
      @Nullable final String editionUriOverride,
      @Nonnull final DenseIdOrder denseIdOrder) {
    final Rf2Files files = locateFiles(releaseRoot);

    log.info("Reading concepts from {}", files.concept);
    final Dataset<Row> conceptRaw = readRf2(files.concept, "id", COLUMN_ACTIVE, "moduleId");
    validateColumns(
        files.concept, conceptRaw, "id", COLUMN_ACTIVE, "moduleId", "definitionStatusId");

    final String version =
        editionUriOverride != null
            ? editionUriOverride
            : detectEditionUri(conceptRaw, files.moduleDependency);
    final String systemVersionId = TerminologyStoreSchema.systemVersionId(SNOMED_URI, version);
    final SnomedVersion parsed = parseVersion(version);
    log.info("Detected SNOMED CT edition {} version {}", parsed.edition, version);

    // Concept dictionary with dense identifiers, ordered by code for determinism.
    final Dataset<Row> codeOrdered =
        conceptRaw
            .select(
                col("id").alias(COLUMN_CODE),
                col(COLUMN_ACTIVE).equalTo("1").alias(COLUMN_ACTIVE),
                col("effectiveTime").alias(COLUMN_EFFECTIVE_TIME),
                col("moduleId").alias(COLUMN_MODULE_ID),
                col("definitionStatusId").equalTo(DEFINED_STATUS).alias(COLUMN_DEFINED))
            .withColumn(COLUMN_DENSE_ID, row_number().over(Window.orderBy(COLUMN_CODE)).minus(1))
            .persist();
    final long conceptCount = codeOrdered.count();

    // The pre-order is derived by permuting the code-order identifiers, so the code ordering is
    // computed either way. Only the permuting variant costs anything extra.
    final Dataset<Row> concepts =
        DenseIdOrder.PRE_ORDER == denseIdOrder
            ? reorderDenseIds(codeOrdered, files, conceptCount)
            : codeOrdered;

    final Dataset<Row> denseByCode = concepts.select(COLUMN_CODE, COLUMN_DENSE_ID);

    // Descriptions and the display term.
    final Descriptions descriptions =
        readDescriptions(files, concepts, denseByCode, systemVersionId);
    final Dataset<Row> conceptTable =
        buildConceptTable(concepts, descriptions.display, systemVersionId);

    // Relationships: is-a feeds the closure, other attributes are stored.
    final Relationships relationships = readRelationships(files, denseByCode, systemVersionId);

    // Reference set membership.
    final Refsets refsets = readRefsets(files, denseByCode, systemVersionId);

    final Dataset<Row> codeSystemRow =
        codeSystemRow(systemVersionId, version, parsed, conceptCount);

    // All parsing succeeded; write the content tables, then the manifest last.
    log.info("Writing {} concepts to the store", conceptCount);
    final TerminologyStoreWriter writer = new TerminologyStoreWriter(spark, storagePath);
    writer.writePartitionedBySystemVersion(codeSystemRow, CODE_SYSTEM, systemVersionId);
    writer.writePartitionedBySystemVersion(conceptTable, CONCEPT, systemVersionId);
    writer.writePartitionedBySystemVersion(descriptions.table, DESCRIPTION, systemVersionId);
    logResolutions(descriptions.resolutions);
    writer.writePartitionedBySystemVersion(relationships.attributes, RELATIONSHIP, systemVersionId);
    logResolutions(relationships.resolutions);
    log.info("Computing the transitive closure");
    final Dataset<Row> closure = new TransitiveClosureBuilder().build(relationships.isa);
    writer.writePartitionedBySystemVersion(closure, CLOSURE, systemVersionId);
    closure.unpersist();
    writer.writePartitionedBySystemVersion(refsets.members, REFSET_MEMBER, systemVersionId);
    logResolutions(refsets.resolutions);
    writeManifest(writer, version, source);
    descriptions.cached.forEach(Dataset::unpersist);
    concepts.unpersist();
    codeOrdered.unpersist();
    log.info("Import complete for {} version {} ({} concepts)", SNOMED_URI, version, conceptCount);
  }

  // --- Dense identifier ordering. ---

  /**
   * Reassigns dense identifiers in depth-first pre-order over the active is-a hierarchy, so that
   * each subtree occupies a near-contiguous interval and the runtime hierarchy index needs
   * materially less memory to represent it.
   *
   * <p>The traversal is computed on the driver from the code-order identifiers already assigned,
   * then broadcast and applied. Collecting the edges rather than distributing the traversal is
   * deliberate: a depth-first order is inherently sequential, and the edge list is small enough to
   * hold on the driver.
   *
   * @param codeOrdered the concept dictionary with code-order dense identifiers
   * @param files the located release files, for the relationship file holding the is-a edges
   * @param conceptCount the number of concepts in the dictionary
   * @return the dictionary with pre-order dense identifiers
   */
  @Nonnull
  private Dataset<Row> reorderDenseIds(
      @Nonnull final Dataset<Row> codeOrdered,
      @Nonnull final Rf2Files files,
      final long conceptCount) {
    if (conceptCount > Integer.MAX_VALUE) {
      throw new TerminologyImportException(
          "Pre-order dense identifiers are not supported for a release of "
              + conceptCount
              + " concepts");
    }
    log.info("Assigning dense identifiers in pre-order over the is-a hierarchy");
    final List<Row> edges = collectIsaEdges(codeOrdered, files);
    final int[] children = new int[edges.size()];
    final int[] parents = new int[edges.size()];
    for (int index = 0; index < edges.size(); index++) {
      children[index] = edges.get(index).getInt(0);
      parents[index] = edges.get(index).getInt(1);
    }
    log.info("Traversing {} active is-a edges", edges.size());
    final int[] permutation = DenseIdPreOrder.compute(children, parents, (int) conceptCount);

    final Broadcast<int[]> broadcast =
        JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(permutation);
    final UDF1<Integer, Integer> lookup = dense -> broadcast.value()[dense];
    final UserDefinedFunction reassign = udf(lookup, DataTypes.IntegerType);
    return codeOrdered.withColumn(COLUMN_DENSE_ID, reassign.apply(col(COLUMN_DENSE_ID))).persist();
  }

  /**
   * Collects the active is-a edges of a release, expressed in code-order dense identifiers.
   *
   * @param codeOrdered the concept dictionary with code-order dense identifiers
   * @param files the located release files
   * @return one row per edge, holding the child's identifier then the parent's
   */
  @Nonnull
  private List<Row> collectIsaEdges(
      @Nonnull final Dataset<Row> codeOrdered, @Nonnull final Rf2Files files) {
    if (files.relationship == null) {
      return List.of();
    }
    final Dataset<Row> denseByCode = codeOrdered.select(COLUMN_CODE, COLUMN_DENSE_ID);
    final Dataset<Row> childDense =
        denseByCode.withColumnRenamed(COLUMN_DENSE_ID, COLUMN_SOURCE_DENSE_ID);
    final Dataset<Row> parentDense =
        denseByCode.withColumnRenamed(COLUMN_DENSE_ID, COLUMN_TARGET_DENSE_ID);
    final Dataset<Row> isaRaw =
        readRf2(files.relationship, "sourceId", "destinationId", "typeId")
            .filter(col(COLUMN_ACTIVE).equalTo("1"))
            .filter(col("typeId").equalTo(IS_A));
    return isaRaw
        .join(childDense, isaRaw.col("sourceId").equalTo(childDense.col(COLUMN_CODE)))
        .join(parentDense, isaRaw.col("destinationId").equalTo(parentDense.col(COLUMN_CODE)))
        .select(col(COLUMN_SOURCE_DENSE_ID), col(COLUMN_TARGET_DENSE_ID))
        .collectAsList();
  }

  // --- Resolution reporting. ---

  /**
   * The row count metrics of one RF2 file: how many active rows it contributed, and how many of
   * them resolved against the concept dictionary. Each metric is named from the file's path,
   * because Spark requires the collected metric names within a query plan to be unique.
   */
  private static final class Resolution {
    @Nonnull final String path;
    @Nonnull final Observation input;
    @Nonnull final Observation resolved;

    Resolution(@Nonnull final String path) {
      this.path = path;
      this.input = new Observation("input: " + path);
      this.resolved = new Observation("resolved: " + path);
    }
  }

  /**
   * Attaches a row count metric to a data frame. The rows are unchanged and no action is added: the
   * count is aggregated as the rows pass through the write the import already performs, so no
   * further pass is made over any RF2 file.
   *
   * @param data the rows to count
   * @param observation the observation that will hold the count
   * @return the same rows, with the metric attached
   */
  @Nonnull
  private static Dataset<Row> observeRowCount(
      @Nonnull final Dataset<Row> data, @Nonnull final Observation observation) {
    return data.observe(observation, count(lit(1)).alias(OBSERVED_ROWS));
  }

  /**
   * Reports, for each file, how many of its active rows resolved against the concept dictionary.
   * This is called once the write that materialises the corresponding table has completed, so the
   * counts are available. A source that is not self-contained, such as a derived package imported
   * without its declared dependency, is a legitimate thing to import, so the shortfall is reported
   * informationally and the import's outcome is unchanged.
   *
   * @param resolutions the resolution metrics to report, one per file
   */
  private static void logResolutions(@Nonnull final List<Resolution> resolutions) {
    // Every metric of a batch is fulfilled by the same query's completion, so they all arrive
    // together or not at all. One deadline therefore bounds the whole batch, rather than a release
    // with dozens of reference set files being able to wait once per file.
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(METRIC_TIMEOUT_SECONDS);
    for (final Resolution resolution : resolutions) {
      final Long input = rowCount(resolution.input, deadline);
      final Long resolved = input == null ? null : rowCount(resolution.resolved, deadline);
      if (input != null && resolved != null) {
        log.info(
            "{}: {} of {} active rows resolved against the concept dictionary.",
            resolution.path,
            resolved,
            input);
      }
    }
  }

  /**
   * Reads an observed row count, waiting no later than a deadline for it to arrive.
   *
   * @param observation the observation to read
   * @param deadline the {@link System#nanoTime()} value to stop waiting at
   * @return the row count, or null if it did not arrive
   */
  @Nullable
  private static Long rowCount(@Nonnull final Observation observation, final long deadline) {
    try {
      final scala.collection.immutable.Map<String, Object> metrics =
          Await.result(
              observation.future(),
              Duration.create(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS));
      final Option<Object> rows = metrics.get(OBSERVED_ROWS);
      return rows.isDefined() ? (Long) rows.get() : null;
    } catch (final TimeoutException e) {
      log.debug("Row count metric '{}' was not reported.", observation.name());
      return null;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  // --- Archive extraction. ---

  /**
   * Reports whether the source points at a zip archive, by its {@code .zip} extension.
   *
   * @param source the source path
   * @return true if the source is a zip archive
   */
  private static boolean isZipArchive(@Nonnull final String source) {
    return source.toLowerCase().endsWith(".zip");
  }

  /**
   * Extracts a zip archive to a fresh local temporary directory, reading the archive through the
   * Hadoop file system so it may reside on any accessible storage. Entries are streamed to disk to
   * bound memory use, and paths are validated to prevent extraction outside the target directory.
   *
   * @param source the path of the zip archive
   * @return the temporary directory containing the extracted release
   * @throws TerminologyImportException if the archive cannot be read or extracted
   */
  @Nonnull
  private java.nio.file.Path extractArchive(@Nonnull final String source) {
    final Path archive = new Path(source);
    try {
      final FileSystem fs = archive.getFileSystem(hadoopConf);
      if (!fs.exists(archive)) {
        throw new TerminologyImportException("RF2 source path does not exist: " + source);
      }
      final java.nio.file.Path target = SecureTempDirectory.create("pathling-rf2-");
      log.info("Extracting RF2 archive {} to {}", source, target);
      try (final ZipInputStream zip =
          new ZipInputStream(new BufferedInputStream(fs.open(archive)))) {
        ZipEntry entry;
        while ((entry = zip.getNextEntry()) != null) {
          if (entry.isDirectory()) {
            continue;
          }
          final java.nio.file.Path destination = target.resolve(entry.getName()).normalize();
          if (!destination.startsWith(target)) {
            throw new TerminologyImportException(
                "Refusing to extract archive entry outside the target directory: "
                    + entry.getName());
          }
          Files.createDirectories(destination.getParent());
          Files.copy(zip, destination, StandardCopyOption.REPLACE_EXISTING);
        }
      }
      return target;
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to extract the RF2 archive at " + source, e);
    }
  }

  /**
   * Deletes a directory tree, used to clean up an extracted archive. Failures are logged rather
   * than thrown, since cleanup should never mask a successful import.
   *
   * @param directory the directory to delete
   */
  private static void deleteRecursively(@Nonnull final java.nio.file.Path directory) {
    try (final Stream<java.nio.file.Path> paths = Files.walk(directory)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (final IOException e) {
                  log.debug("Failed to delete temporary file {}", path, e);
                }
              });
    } catch (final IOException e) {
      log.warn("Failed to clean up temporary extraction directory {}", directory, e);
    }
  }

  // --- File discovery. ---

  /** The RF2 files located within a release's Snapshot directory. */
  private static final class Rf2Files {
    @Nonnull final String concept;
    @Nonnull final List<String> descriptions;
    @Nullable final String relationship;
    @Nonnull final List<String> languages;
    @Nullable final String moduleDependency;
    @Nonnull final List<String> otherRefsets;

    Rf2Files(
        @Nonnull final String concept,
        @Nonnull final List<String> descriptions,
        @Nullable final String relationship,
        @Nonnull final List<String> languages,
        @Nullable final String moduleDependency,
        @Nonnull final List<String> otherRefsets) {
      this.concept = concept;
      this.descriptions = descriptions;
      this.relationship = relationship;
      this.languages = languages;
      this.moduleDependency = moduleDependency;
      this.otherRefsets = otherRefsets;
    }
  }

  @Nonnull
  private Rf2Files locateFiles(@Nonnull final String source) {
    final Path root = new Path(source);
    final List<String> concepts = new ArrayList<>();
    final List<String> descriptions = new ArrayList<>();
    final List<String> relationships = new ArrayList<>();
    final List<String> languages = new ArrayList<>();
    final List<String> moduleDependencies = new ArrayList<>();
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
          concepts.add(path);
        } else if (name.startsWith("sct2_Description_")
            || name.startsWith("sct2_TextDefinition_")) {
          descriptions.add(path);
        } else if (name.startsWith("sct2_Relationship_")) {
          relationships.add(path);
        } else if (name.contains("Refset_Language")) {
          languages.add(path);
        } else if (name.contains("Refset_ModuleDependency")) {
          moduleDependencies.add(path);
        } else if (name.startsWith("der2_") && name.contains("Refset")) {
          otherRefsets.add(path);
        }
      }
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to read the RF2 source at " + source, e);
    }

    if (concepts.isEmpty()) {
      final String detail =
          sawNonSnapshotRelease
              ? " Only snapshot releases are supported; this appears to be a full or delta release."
              : "";
      throw new TerminologyImportException(
          "No SNOMED CT snapshot concept file was found under " + source + "." + detail);
    }
    // The concept, relationship and module dependency roles are single-valued. More than one file
    // filling one of them means two release trees have been placed in the same directory, in which
    // case the import would otherwise proceed against one tree's content and silently ignore the
    // other's.
    requireSingle("concept", concepts, source);
    requireSingle("relationship", relationships, source);
    requireSingle("module dependency", moduleDependencies, source);
    return new Rf2Files(
        concepts.get(0),
        descriptions,
        relationships.isEmpty() ? null : relationships.get(0),
        languages,
        moduleDependencies.isEmpty() ? null : moduleDependencies.get(0),
        otherRefsets);
  }

  /**
   * Rejects a source in which more than one file fills a single-valued role, naming every candidate
   * in a stable order so the offending files can be found. No file content has been read at this
   * point, so the store is untouched.
   *
   * @param role the name of the role, as it appears in the failure message
   * @param candidates the paths of the files found for the role
   * @param source the source path the release was discovered under
   * @throws TerminologyImportException if more than one candidate was found
   */
  private static void requireSingle(
      @Nonnull final String role,
      @Nonnull final List<String> candidates,
      @Nonnull final String source) {
    if (candidates.size() > 1) {
      final List<String> sorted = new ArrayList<>(candidates);
      sorted.sort(Comparator.naturalOrder());
      throw new TerminologyImportException(
          "Multiple SNOMED CT snapshot "
              + role
              + " files were found under "
              + source
              + ": "
              + String.join(", ", sorted)
              + ". A single "
              + role
              + " file is expected. If you are combining releases, concatenate them into one file"
              + " rather than placing both release trees in the same directory.");
    }
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

    /**
     * The cached source data frames feeding {@link #table} and {@link #display}, released once both
     * have been written.
     */
    @Nonnull final List<Dataset<Row>> cached;

    /** The resolution metrics of each description and text definition file read. */
    @Nonnull final List<Resolution> resolutions;

    Descriptions(
        @Nonnull final Dataset<Row> table,
        @Nonnull final Dataset<Row> display,
        @Nonnull final List<Dataset<Row>> cached,
        @Nonnull final List<Resolution> resolutions) {
      this.table = table;
      this.display = display;
      this.cached = cached;
      this.resolutions = resolutions;
    }
  }

  @Nonnull
  private Descriptions readDescriptions(
      @Nonnull final Rf2Files files,
      @Nonnull final Dataset<Row> concepts,
      @Nonnull final Dataset<Row> denseByCode,
      @Nonnull final String systemVersionId) {
    if (files.descriptions.isEmpty()) {
      final Dataset<Row> emptyTable = emptyDescriptionTable();
      final Dataset<Row> emptyDisplay =
          concepts
              .select(col(COLUMN_CODE), lit(null).cast("string").alias(COLUMN_DISPLAY))
              .limit(0);
      return new Descriptions(emptyTable, emptyDisplay, List.of(), List.of());
    }

    // The description and language reference set files are each consumed several times below (the
    // description table, the preferred synonym, and the FSN all read the descriptions; the
    // acceptability map and the preferred flag both read the language refset). They are the largest
    // files in a release, so the parsed rows are cached to read and parse each file only once. A
    // release may ship several files of each kind (descriptions plus text definitions, one language
    // file per dialect), so all of them are combined.
    //
    // Each file is carried separately through the join to the concept dictionary so that its own
    // share of the rows that resolved can be reported, and the joined results are then combined.
    // An inner join distributes over a union, so the same rows pass through the same join as they
    // would have done had the files been combined first and joined once.
    final List<Dataset<Row>> descriptionFiles = new ArrayList<>();
    final List<Dataset<Row>> resolvedFiles = new ArrayList<>();
    final List<Resolution> resolutions = new ArrayList<>();
    for (final String path : files.descriptions) {
      final Dataset<Row> active =
          readRf2(path, "id", "conceptId", "typeId", COLUMN_TERM)
              .filter(col(COLUMN_ACTIVE).equalTo("1"))
              .persist();
      descriptionFiles.add(active);
      final Resolution resolution = new Resolution(path);
      resolutions.add(resolution);
      // The metrics are attached above the cache boundary, so that they are collected by the write
      // of the description table rather than by whichever action first populates the cache.
      final Dataset<Row> observed = observeRowCount(active, resolution.input);
      final Dataset<Row> resolved =
          observed
              .join(denseByCode, observed.col("conceptId").equalTo(denseByCode.col(COLUMN_CODE)))
              .select(
                  observed.col("id").alias(DESCRIPTION_ID),
                  col(COLUMN_DENSE_ID),
                  observed.col(COLUMN_TERM),
                  observed.col("languageCode").alias(COLUMN_LANGUAGE),
                  observed.col("typeId").alias(COLUMN_TYPE_CODE));
      resolvedFiles.add(observeRowCount(resolved, resolution.resolved));
    }
    final Dataset<Row> descRaw =
        descriptionFiles.stream().reduce(Dataset::unionByName).orElseThrow();
    final Dataset<Row> resolvedDescriptions =
        resolvedFiles.stream().reduce(Dataset::unionByName).orElseThrow();

    // Active language reference set rows: description id -> (refset, acceptability).
    final Dataset<Row> langActive =
        files.languages.stream()
            .map(path -> readRf2(path, "referencedComponentId", "refsetId", ACCEPTABILITY_ID))
            .reduce(Dataset::unionByName)
            .map(
                combined ->
                    combined
                        .filter(col(COLUMN_ACTIVE).equalTo("1"))
                        .select(
                            col("referencedComponentId").alias("descId"),
                            col("refsetId"),
                            col(ACCEPTABILITY_ID)))
            .orElseGet(this::emptyLanguage)
            .persist();

    // Acceptability map per description.
    final Dataset<Row> acceptability =
        langActive
            .groupBy("descId")
            .agg(
                map_from_entries(collect_list(struct(col("refsetId"), col(ACCEPTABILITY_ID))))
                    .alias(COLUMN_ACCEPTABILITY));

    // The acceptability map is a left join keyed on the description, so it neither adds nor drops
    // rows and the resolved count above remains the count written to the table.
    final Dataset<Row> table =
        resolvedDescriptions
            .join(
                acceptability,
                resolvedDescriptions.col(DESCRIPTION_ID).equalTo(acceptability.col("descId")),
                "left_outer")
            .select(
                lit(systemVersionId).alias(COLUMN_SYSTEM_VERSION_ID),
                col(COLUMN_DENSE_ID).alias(COLUMN_CONCEPT_DENSE_ID),
                col(COLUMN_TERM),
                col(COLUMN_LANGUAGE),
                col(COLUMN_TYPE_CODE),
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

    final List<Dataset<Row>> cached = new ArrayList<>(descriptionFiles);
    cached.add(langActive);
    return new Descriptions(table, display, cached, resolutions);
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

    /** The resolution metrics of the relationship file, empty when the release ships none. */
    @Nonnull final List<Resolution> resolutions;

    Relationships(
        @Nonnull final Dataset<Row> isa,
        @Nonnull final Dataset<Row> attributes,
        @Nonnull final List<Resolution> resolutions) {
      this.isa = isa;
      this.attributes = attributes;
      this.resolutions = resolutions;
    }
  }

  @Nonnull
  private Relationships readRelationships(
      @Nonnull final Rf2Files files,
      @Nonnull final Dataset<Row> denseByCode,
      @Nonnull final String systemVersionId) {
    if (files.relationship == null) {
      return new Relationships(emptyIsa(), emptyRelationshipTable(), List.of());
    }
    final Resolution resolution = new Resolution(files.relationship);
    final Dataset<Row> relRaw =
        observeRowCount(
            readRf2(files.relationship, "sourceId", "destinationId", "typeId")
                .filter(col(COLUMN_ACTIVE).equalTo("1")),
            resolution.input);

    final Dataset<Row> sourceDense =
        denseByCode.withColumnRenamed(COLUMN_DENSE_ID, COLUMN_SOURCE_DENSE_ID);
    final Dataset<Row> targetDense =
        denseByCode.withColumnRenamed(COLUMN_DENSE_ID, COLUMN_TARGET_DENSE_ID);

    // A relationship resolves only when both its source and its destination concept are present,
    // so the resolved count is taken after both joins.
    final Dataset<Row> mapped =
        observeRowCount(
                relRaw
                    .join(sourceDense, relRaw.col("sourceId").equalTo(sourceDense.col(COLUMN_CODE)))
                    .join(
                        targetDense,
                        relRaw.col("destinationId").equalTo(targetDense.col(COLUMN_CODE)))
                    .select(
                        col("typeId"),
                        col("relationshipGroup"),
                        col(COLUMN_SOURCE_DENSE_ID),
                        col(COLUMN_TARGET_DENSE_ID)),
                resolution.resolved)
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
    return new Relationships(isa, attributes, List.of(resolution));
  }

  // --- Reference sets. ---

  /** The reference set membership rows, and the resolution metrics of each file read. */
  private static final class Refsets {
    @Nonnull final Dataset<Row> members;
    @Nonnull final List<Resolution> resolutions;

    Refsets(@Nonnull final Dataset<Row> members, @Nonnull final List<Resolution> resolutions) {
      this.members = members;
      this.resolutions = resolutions;
    }
  }

  @Nonnull
  private Refsets readRefsets(
      @Nonnull final Rf2Files files,
      @Nonnull final Dataset<Row> denseByCode,
      @Nonnull final String systemVersionId) {
    Dataset<Row> members = null;
    final List<Resolution> resolutions = new ArrayList<>();
    for (final String path : files.otherRefsets) {
      final Resolution resolution = new Resolution(path);
      resolutions.add(resolution);
      final Dataset<Row> raw =
          observeRowCount(
              readRf2(path, "refsetId", "referencedComponentId")
                  .filter(col(COLUMN_ACTIVE).equalTo("1")),
              resolution.input);
      final boolean hasTarget = raw.schema().getFieldIndex(TARGET_COMPONENT_ID).isDefined();
      final Dataset<Row> targetColumn =
          hasTarget
              ? raw.withColumn(COLUMN_TARGET_CODE, col(TARGET_COMPONENT_ID))
              : raw.withColumn(COLUMN_TARGET_CODE, lit(null).cast(DataTypes.StringType));
      final Dataset<Row> mapped =
          observeRowCount(
              targetColumn
                  .join(
                      denseByCode,
                      targetColumn
                          .col("referencedComponentId")
                          .equalTo(denseByCode.col(COLUMN_CODE)))
                  .select(
                      lit(systemVersionId).alias(COLUMN_SYSTEM_VERSION_ID),
                      col("refsetId").alias(COLUMN_REFSET_CODE),
                      col(COLUMN_DENSE_ID).alias(COLUMN_REFERENCED_DENSE_ID),
                      col(COLUMN_TARGET_CODE)),
              resolution.resolved);
      members = members == null ? mapped : members.unionByName(mapped);
    }
    return new Refsets(members == null ? emptyRefsetTable(systemVersionId) : members, resolutions);
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
    writer.upsertManifestEntry(
        new ManifestEntry(
            TerminologyStoreSchema.STORE_FORMAT_VERSION,
            "code_system",
            SNOMED_URI,
            version,
            source,
            Instant.now()));
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
  private String detectEditionUri(
      @Nonnull final Dataset<Row> conceptRaw, @Nullable final String moduleDependencyPath) {
    final Map<String, Long> conceptCounts = new HashMap<>();
    for (final Row row :
        conceptRaw
            .filter(col(COLUMN_ACTIVE).equalTo("1"))
            .groupBy("moduleId")
            .count()
            .collectAsList()) {
      conceptCounts.put(row.getString(0), row.getLong(1));
    }
    final String dependencyModule =
        moduleDependencyPath != null
            ? editionModuleFromDependencies(moduleDependencyPath, conceptCounts)
            : null;
    final String module =
        dependencyModule != null ? dependencyModule : majorityModule(conceptCounts);
    final String effectiveTime =
        conceptRaw.agg(org.apache.spark.sql.functions.max("effectiveTime")).first().getString(0);
    return SNOMED_URI + "/" + module + "/version/" + effectiveTime;
  }

  /**
   * Determines the edition module from the module dependency reference set. The edition module is
   * the concept-bearing module whose transitive dependencies reach the most other concept-bearing
   * modules; a derived edition depends on the modules it extends, while side modules at the top of
   * the graph (such as the International ICD-10 mapping module) carry no concepts. Ties are broken
   * by concept count and then by module identifier, and the method returns null when the reference
   * set has no active rows, in which case detection falls back to the most frequent concept module.
   */
  @Nullable
  private String editionModuleFromDependencies(
      @Nonnull final String moduleDependencyPath, @Nonnull final Map<String, Long> conceptCounts) {
    final List<Row> dependencies =
        readRf2(moduleDependencyPath, COLUMN_ACTIVE, "moduleId", "referencedComponentId")
            .filter(col(COLUMN_ACTIVE).equalTo("1"))
            .select("moduleId", "referencedComponentId")
            .distinct()
            .collectAsList();
    if (dependencies.isEmpty()) {
      return null;
    }
    final Map<String, Set<String>> dependsOn = new HashMap<>();
    for (final Row dependency : dependencies) {
      dependsOn
          .computeIfAbsent(dependency.getString(0), key -> new HashSet<>())
          .add(dependency.getString(1));
    }
    return conceptCounts.keySet().stream()
        .max(
            Comparator.<String>comparingLong(
                    module ->
                        reachableFrom(module, dependsOn).stream()
                            .filter(
                                reached ->
                                    !reached.equals(module) && conceptCounts.containsKey(reached))
                            .count())
                .thenComparingLong(conceptCounts::get)
                .thenComparing(Comparator.reverseOrder()))
        .orElse(null);
  }

  /** Returns the set of modules transitively reachable from {@code module} via dependencies. */
  @Nonnull
  private static Set<String> reachableFrom(
      @Nonnull final String module, @Nonnull final Map<String, Set<String>> dependsOn) {
    final Set<String> reached = new HashSet<>();
    final ArrayDeque<String> frontier = new ArrayDeque<>();
    frontier.add(module);
    while (!frontier.isEmpty()) {
      final String current = frontier.remove();
      for (final String next : dependsOn.getOrDefault(current, Set.of())) {
        if (reached.add(next)) {
          frontier.add(next);
        }
      }
    }
    return reached;
  }

  /** Returns the most frequent active concept module, as a fallback edition heuristic. */
  @Nonnull
  private static String majorityModule(@Nonnull final Map<String, Long> conceptCounts) {
    return conceptCounts.entrySet().stream()
        .max(
            Map.Entry.<String, Long>comparingByValue()
                .thenComparing(Map.Entry.comparingByKey(Comparator.reverseOrder())))
        .orElseThrow(
            () ->
                new TerminologyImportException(
                    "The release contains no active concepts, so no edition can be detected."))
        .getKey();
  }

  @Nonnull
  private SnomedVersion parseVersion(@Nonnull final String version) {
    final Matcher matcher = SNOMED_VERSION.matcher(version);
    if (matcher.find()) {
      return new SnomedVersion(matcher.group("edition"), matcher.group("time"));
    }
    return new SnomedVersion(null, null);
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
