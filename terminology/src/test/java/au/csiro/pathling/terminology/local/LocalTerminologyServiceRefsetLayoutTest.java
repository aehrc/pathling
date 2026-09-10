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

package au.csiro.pathling.terminology.local;

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_REFERENCED_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_REFSET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.REFSET_MEMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import au.csiro.pathling.terminology.store.TerminologyStoreSchema;
import au.csiro.pathling.terminology.store.TerminologyStoreWriter;
import au.csiro.pathling.test.NoNetworkExtension;
import au.csiro.pathling.test.Rf2Mini;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that reverse translation through a SNOMED association reference set is ordered by the
 * reference set's content rather than by the order the store's rows happen to be laid out in.
 *
 * <p>An imported store cannot exhibit the defect on its own. The importer resolves reference set
 * rows against the concept dictionary with a join whose streamed side, at this fixture's size, is
 * the dictionary, so the rows land in concept code order however the release file was written. That
 * is an accident of the chosen join strategy and not a guarantee, so this class takes a copy of the
 * shared fixture store and rewrites its reference set table with the rows in descending code order
 * - a layout another join strategy could legitimately produce. The layout is asserted before the
 * behaviour is, so the test cannot quietly stop testing anything.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class LocalTerminologyServiceRefsetLayoutTest {

  private static final String SAME_AS_CONCEPT_MAP =
      Rf2Mini.SNOMED_URI + "?fhir_cm=" + Rf2Mini.SAME_AS_REFSET;

  /** The concepts the fixture associates with {@link Rf2Mini#TYPE2_DIABETES}, in code order. */
  private static final List<String> ASSOCIATED_IN_CODE_ORDER =
      List.of(
          Rf2Mini.DIABETES_INACTIVE,
          Rf2Mini.ASSOCIATED_FILLER_1,
          Rf2Mini.ASSOCIATED_FILLER_2,
          Rf2Mini.ASSOCIATED_FILLER_3);

  private static TerminologyStoreReader reader;
  private static LocalTerminologyService service;

  @BeforeAll
  static void setUp(@TempDir final Path storeDir) {
    final Path store = storeDir.resolve("store");
    copyStore(Path.of(LocalTerminologyFixture.storagePath()), store);
    reverseRefsetRowOrder(store.toString());

    reader = TerminologyStoreReader.open(store.toString(), Map.of());
    final TerminologyConfiguration configuration =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(LocalTerminologyConfiguration.builder().storagePath(store.toString()).build())
            .build();
    service = new LocalTerminologyService(configuration, Map.of());
  }

  @AfterAll
  static void tearDown() {
    if (service != null) {
      service.close();
      service = null;
    }
  }

  /**
   * Rewrites the store's reference set table with its rows in descending referenced concept order,
   * which for this fixture is descending concept code order.
   */
  private static void reverseRefsetRowOrder(@Nonnull final String store) {
    final SparkSession spark =
        SparkSession.builder()
            .appName("LocalTerminologyServiceRefsetLayoutTest")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    final Dataset<Row> existing =
        spark.read().format("delta").load(TerminologyStoreSchema.tablePath(store, REFSET_MEMBER));
    // The rows are collected before the table is overwritten, because a lazily evaluated read of
    // the same path would otherwise race with the write.
    final List<Row> rows = new ArrayList<>(existing.collectAsList());
    rows.sort(
        Comparator.comparingInt((final Row row) -> row.getAs(COLUMN_REFERENCED_DENSE_ID))
            .reversed());
    // A single partition keeps the collected order intact through the write, so the table's
    // physical row order is exactly the order of this list.
    final Dataset<Row> reversed = spark.createDataFrame(rows, existing.schema()).coalesce(1);
    new TerminologyStoreWriter(spark, store)
        .writeTable(reversed, REFSET_MEMBER, SaveMode.Overwrite, List.of(COLUMN_SYSTEM_VERSION_ID));
  }

  private static void copyStore(@Nonnull final Path source, @Nonnull final Path destination) {
    try (final Stream<Path> tree = Files.walk(source)) {
      for (final Path path : tree.toList()) {
        final Path target = destination.resolve(source.relativize(path).toString());
        if (Files.isDirectory(path)) {
          Files.createDirectories(target);
        } else {
          Files.createDirectories(target.getParent());
          Files.copy(path, target);
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The codes of the concepts referenced by the association reference set, in physical order. */
  @Nonnull
  private static List<String> physicalRowOrder() {
    final Map<Integer, String> codeByDense = new HashMap<>();
    reader.readTable(
        CONCEPT, row -> codeByDense.put(row.getInt(COLUMN_DENSE_ID), row.getString(COLUMN_CODE)));
    final List<String> codes = new ArrayList<>();
    reader.readTable(
        REFSET_MEMBER,
        row -> {
          if (Rf2Mini.SAME_AS_REFSET.equals(row.getString(COLUMN_REFSET_CODE))) {
            codes.add(codeByDense.get(row.getInt(COLUMN_REFERENCED_DENSE_ID)));
          }
        });
    return codes;
  }

  @Test
  void laysTheReferenceSetOutInDescendingOrder() {
    // The premise of the test below: if this store ever stops being laid out against the expected
    // order, the ordering assertion is no longer load-bearing and this test says so.
    assertEquals(ASSOCIATED_IN_CODE_ORDER.reversed(), physicalRowOrder());
  }

  @Test
  void reverseTranslationIgnoresThePhysicalRowOrder() {
    final List<Translation> result =
        service.translate(
            new Coding().setSystem(Rf2Mini.SNOMED_URI).setCode(Rf2Mini.TYPE2_DIABETES),
            SAME_AS_CONCEPT_MAP,
            true,
            null);
    assertEquals(
        ASSOCIATED_IN_CODE_ORDER,
        result.stream().map(translation -> translation.getConcept().getCode()).toList());
  }
}
