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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.test.Rf2Mini;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * Verifies how the importer settles each concept's stored display. The dialect named by the import
 * option decides it; failing that, the release decides it where it can; and failing both, the
 * import is refused before anything is written, listing the language reference sets the operator
 * can choose between. The stored display never depends on the order the release's rows were laid
 * out in.
 *
 * @author John Grimes
 */
class SnomedRf2ImporterDialectTest {

  /** The GB English dialect named as the extension form of its reference set. */
  private static final String GB_EXTENSION_TAG = "en-x-sctlang-90000000-00005080-04";

  // RF2 metadata identifiers used when editing a copy of the fixture.
  private static final String FSN_TYPE = "900000000000003001";
  private static final String SYNONYM_TYPE = "900000000000013009";
  private static final String PREFERRED_ACCEPTABILITY = "900000000000548007";
  private static final String CASE_INSENSITIVE = "900000000000448009";

  private static SparkSession spark;
  private static Path storeDir;
  private static Path workDir;

  @BeforeAll
  static void setUp(
      @TempDir final Path warehouse, @TempDir final Path stores, @TempDir final Path work) {
    storeDir = stores;
    workDir = work;
    spark =
        SparkSession.builder()
            .appName("SnomedRf2ImporterDialectTest")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", warehouse.toString())
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  // --- The option decides. ---

  @Test
  void storesTheDisplaysOfTheNamedDialect() {
    // The International default is US English, so naming GB English is what makes the difference
    // observable: the three divergent concepts take their GB terms, and every other concept takes
    // the one term both reference sets prefer.
    final Map<String, String> gb =
        displaysOf(importInto("named-gb", Rf2Mini.baseRelease(), "en-GB"));
    assertEquals(Rf2Mini.DIVERGENT_GB_ENDOCRINE, gb.get(Rf2Mini.ENDOCRINE_STRUCTURE));
    assertEquals(Rf2Mini.DIVERGENT_GB_PANCREAS, gb.get(Rf2Mini.PANCREAS_STRUCTURE));
    assertEquals(Rf2Mini.DIVERGENT_GB_DEGENERATION, gb.get(Rf2Mini.DEGENERATION_MORPH));
    assertEquals("Diabetes mellitus", gb.get(Rf2Mini.DIABETES));
  }

  @Test
  void storesTheSameDisplaysWhenTheDialectIsNamedByIdentifierOrByExtensionTag() {
    // The three ways of naming one dialect must be interchangeable.
    final Map<String, String> byTag =
        displaysOf(importInto("by-tag", Rf2Mini.baseRelease(), "en-GB"));
    final Map<String, String> byIdentifier =
        displaysOf(importInto("by-id", Rf2Mini.baseRelease(), Rf2Mini.GB_ENGLISH_REFSET));
    final Map<String, String> byExtension =
        displaysOf(importInto("by-extension", Rf2Mini.baseRelease(), GB_EXTENSION_TAG));
    assertEquals(byTag, byIdentifier);
    assertEquals(byTag, byExtension);
  }

  @Test
  void refusesADialectTheReleaseHoldsNoReferenceSetFor() {
    final String store = storeDir.resolve("missing-dialect").toString();
    final TerminologyImportException failure =
        assertThrows(
            TerminologyImportException.class,
            () ->
                new SnomedRf2Importer(spark, store)
                    .importFrom(
                        Rf2Mini.baseRelease().toString(), null, DenseIdOrder.CODE_ORDER, "es"));
    assertTrue(failure.getMessage().contains("'es'"), failure.getMessage());
    assertTrue(failure.getMessage().contains("448879004"), failure.getMessage());
    assertNothingWritten(store);
  }

  @Test
  void refusesADialectThatNamesNoReferenceSetAtAll() {
    final String store = storeDir.resolve("unknown-dialect").toString();
    final TerminologyImportException failure =
        assertThrows(
            TerminologyImportException.class,
            () ->
                new SnomedRf2Importer(spark, store)
                    .importFrom(
                        Rf2Mini.baseRelease().toString(),
                        null,
                        DenseIdOrder.CODE_ORDER,
                        "not-a-dialect"));
    assertTrue(failure.getMessage().contains("'not-a-dialect'"), failure.getMessage());
    assertNothingWritten(store);
  }

  // --- The release decides. ---

  @Test
  void storesUsEnglishDisplaysForAnUnnamedInternationalImport() {
    // The base release holds two language reference sets and is the International edition, so US
    // English is chosen. This is also the behaviour every other test in the suite depends on.
    final Map<String, String> displays =
        displaysOf(importInto("unnamed-international", Rf2Mini.baseRelease(), null));
    assertEquals(Rf2Mini.DIVERGENT_US_ENDOCRINE, displays.get(Rf2Mini.ENDOCRINE_STRUCTURE));
    assertEquals(Rf2Mini.DIVERGENT_US_PANCREAS, displays.get(Rf2Mini.PANCREAS_STRUCTURE));
    assertEquals(Rf2Mini.DIVERGENT_US_DEGENERATION, displays.get(Rf2Mini.DEGENERATION_MORPH));
  }

  @Test
  void storesTheSoleReferenceSetsDisplaysForAnUnnamedImport() {
    // A release holding exactly one language reference set leaves no room for doubt, so no option
    // is
    // needed. The copy keeps only the GB rows of the language file, which makes GB English the sole
    // reference set and therefore the source of every display.
    final Path release = releaseWithOnlyOneLanguageReferenceSet("sole-refset");
    final Map<String, String> displays = displaysOf(importInto("sole-refset", release, null));
    assertEquals(Rf2Mini.DIVERGENT_GB_ENDOCRINE, displays.get(Rf2Mini.ENDOCRINE_STRUCTURE));
    assertEquals(Rf2Mini.DIVERGENT_GB_PANCREAS, displays.get(Rf2Mini.PANCREAS_STRUCTURE));
  }

  // --- Neither decides. ---

  @Test
  void refusesAnAmbiguousReleaseBeforeWritingAnything() {
    // The national release holds three language reference sets and is not the International
    // edition,
    // so no rule can choose between them. The failure names every candidate, by identifier and by
    // the name the release itself gives that reference set concept, in ascending identifier order.
    final String store = storeDir.resolve("ambiguous").toString();
    final TerminologyImportException failure =
        assertThrows(
            TerminologyImportException.class,
            () ->
                new SnomedRf2Importer(spark, store)
                    .importFrom(Rf2Mini.nationalRelease().toString(), null));
    assertEquals(
        """
        The release holds 3 language reference sets and none of them is a clear default. \
        Name one with the defaultDialect import option:
          1999011  Mini national English language reference set (foundation metadata concept)
          900000000000508004  Great Britain English language reference set (foundation metadata \
        concept)
          900000000000509007  United States of America English language reference set (foundation \
        metadata concept)\
        """,
        failure.getMessage());
    assertNothingWritten(store);
  }

  @Test
  void importsAnAmbiguousReleaseOnceOneOfTheListedReferenceSetsIsNamed() {
    // The message exists to be acted on, so naming one of the identifiers it lists must succeed.
    final Map<String, String> displays =
        displaysOf(
            importInto("ambiguous-resolved", Rf2Mini.nationalRelease(), Rf2Mini.GB_ENGLISH_REFSET));
    assertEquals(Rf2Mini.DIVERGENT_GB_PANCREAS, displays.get(Rf2Mini.PANCREAS_STRUCTURE));
    assertEquals("Diabetes mellitus", displays.get(Rf2Mini.DIABETES));
  }

  @Test
  void reportsTheChosenDialectAndHowItWasDetermined() throws Throwable {
    // The operator has to be able to see which dialect the store's displays came from, and why, so
    // each of the three derivations reports itself.
    final List<ILoggingEvent> named = new ArrayList<>();
    captureDialectLog(
        named, () -> importInto("log-named", Rf2Mini.baseRelease(), Rf2Mini.GB_ENGLISH_REFSET));
    assertTrue(
        messages(named).stream()
            .anyMatch(
                message ->
                    message.contains("Using language reference set " + Rf2Mini.GB_ENGLISH_REFSET)
                        && message.contains("named by the defaultDialect import option")),
        messages(named).toString());

    final List<ILoggingEvent> derived = new ArrayList<>();
    captureDialectLog(derived, () -> importInto("log-derived", Rf2Mini.baseRelease(), null));
    assertTrue(
        messages(derived).stream()
            .anyMatch(
                message ->
                    message.contains("Using language reference set " + Rf2Mini.US_ENGLISH_REFSET)
                        && message.contains("International edition")),
        messages(derived).toString());

    final List<ILoggingEvent> sole = new ArrayList<>();
    final Path release = releaseWithOnlyOneLanguageReferenceSet("log-sole-release");
    captureDialectLog(sole, () -> importInto("log-sole", release, null));
    assertTrue(
        messages(sole).stream()
            .anyMatch(
                message ->
                    message.contains("Using language reference set " + Rf2Mini.GB_ENGLISH_REFSET)
                        && message.contains("the only one this release holds")),
        messages(sole).toString());
  }

  // --- The fallback chain. ---

  @Test
  void fallsThroughToAnotherReferenceSetThenTheFullySpecifiedNameThenTheCode() {
    // A concept the default reference set marks no preferred synonym for takes the preferred
    // synonym
    // of the lowest-numbered other reference set; one with no preferred synonym anywhere takes its
    // fully specified name; and one with neither takes its own code.
    final Path release = releaseWithProgressivelyStrippedTerms();
    final Map<String, String> displays = displaysOf(importInto("fallback", release, "en-US"));
    // PANCREAS_STRUCTURE keeps only its GB-preferred synonym, so the GB reference set answers.
    assertEquals(Rf2Mini.DIVERGENT_GB_PANCREAS, displays.get(Rf2Mini.PANCREAS_STRUCTURE));
    // DEGENERATION_MORPH keeps no synonym at all, so its fully specified name answers.
    assertEquals(
        "Degeneration (morphologic abnormality)", displays.get(Rf2Mini.DEGENERATION_MORPH));
    // MORPHOLOGY_TOP keeps no description at all, so its code answers.
    assertEquals(Rf2Mini.MORPHOLOGY_TOP, displays.get(Rf2Mini.MORPHOLOGY_TOP));
  }

  @Test
  void resolvesTwoSynonymsMarkedPreferredInOneReferenceSetIdenticallyOnEveryImport() {
    // RF2 should not mark two synonyms preferred within one language reference set, but a data
    // error
    // does. The alphabetically first is taken, so two imports of the same release still agree.
    final Path release = releaseWithTwoPreferredSynonyms();
    final Map<String, String> first = displaysOf(importInto("two-preferred-1", release, "en-US"));
    final Map<String, String> second = displaysOf(importInto("two-preferred-2", release, "en-US"));
    assertEquals(first, second);
    // "A duplicate preferred term" sorts before "Pancreatic structure".
    assertEquals("A duplicate preferred term", first.get(Rf2Mini.PANCREAS_STRUCTURE));
  }

  // --- Helpers. ---

  /** Imports a release into a named store beneath the shared temporary directory. */
  @Nonnull
  private static String importInto(
      @Nonnull final String name, @Nonnull final Path release, final String defaultDialect) {
    final String store = storeDir.resolve(name).toString();
    new SnomedRf2Importer(spark, store)
        .importFrom(release.toString(), null, DenseIdOrder.CODE_ORDER, defaultDialect);
    return store;
  }

  /** Reads the stored display of every concept in a store, keyed by concept code. */
  @Nonnull
  private static Map<String, String> displaysOf(@Nonnull final String storagePath) {
    final Map<String, String> displays = new TreeMap<>();
    TerminologyStoreReader.open(storagePath, Map.of())
        .readTable(
            CONCEPT,
            row -> displays.put(row.getString(COLUMN_CODE), row.getString(COLUMN_DISPLAY)));
    return displays;
  }

  /** Asserts that a refused import left no content behind. */
  private static void assertNothingWritten(@Nonnull final String storagePath) {
    final Path store = Path.of(storagePath);
    if (!Files.exists(store)) {
      return;
    }
    try (final Stream<Path> contents = Files.walk(store)) {
      assertFalse(
          contents.anyMatch(path -> path.getFileName().toString().endsWith(".parquet")),
          "The refused import wrote table content to " + storagePath);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Copies the base release, keeping only the GB English rows of its language reference set file,
   * so that the release holds exactly one language reference set.
   */
  @Nonnull
  private static Path releaseWithOnlyOneLanguageReferenceSet(@Nonnull final String name) {
    final Path release = copyOfBaseRelease(name);
    rewriteLanguageFile(
        release,
        rows -> rows.stream().filter(row -> row.contains(Rf2Mini.GB_ENGLISH_REFSET)).toList());
    return release;
  }

  /**
   * Copies the base release and removes terms progressively from three concepts, so that each falls
   * a further step down the display chain: the pancreas keeps only its GB-preferred synonym, the
   * degeneration morphology keeps only its fully specified name, and the morphology root keeps no
   * description at all.
   */
  @Nonnull
  private static Path releaseWithProgressivelyStrippedTerms() {
    final Path release = copyOfBaseRelease("fallback");
    // Drop the US-preferred synonym of the pancreas, every synonym of the degeneration morphology,
    // and every description of the morphology root.
    rewriteDescriptionFile(
        release,
        rows ->
            rows.stream()
                .filter(row -> !isDescriptionOf(row, Rf2Mini.MORPHOLOGY_TOP))
                .filter(
                    row ->
                        !isDescriptionOf(row, Rf2Mini.PANCREAS_STRUCTURE)
                            || !row.contains("\t" + Rf2Mini.DIVERGENT_US_PANCREAS + "\t"))
                .filter(
                    row ->
                        !isDescriptionOf(row, Rf2Mini.DEGENERATION_MORPH)
                            || row.contains("\t" + FSN_TYPE + "\t"))
                .toList());
    // Language rows referring to a description that no longer exists resolve to nothing, so the
    // language file needs no editing.
    return release;
  }

  /**
   * Copies the base release and marks a second synonym of the pancreas preferred within the US
   * English reference set, which RF2 should never do.
   */
  @Nonnull
  private static Path releaseWithTwoPreferredSynonyms() {
    final Path release = copyOfBaseRelease("two-preferred");
    final String duplicateId = "9000101";
    rewriteDescriptionFile(
        release,
        rows -> {
          final List<String> extended = new ArrayList<>(rows);
          extended.add(
              String.join(
                  "\t",
                  duplicateId,
                  "20230601",
                  "1",
                  Rf2Mini.CORE_MODULE,
                  Rf2Mini.PANCREAS_STRUCTURE,
                  "en",
                  SYNONYM_TYPE,
                  "A duplicate preferred term",
                  CASE_INSENSITIVE));
          return extended;
        });
    rewriteLanguageFile(
        release,
        rows -> {
          final List<String> extended = new ArrayList<>(rows);
          extended.add(
              String.join(
                  "\t",
                  "00000000-0000-4000-8000-ffffffffffff",
                  "20230601",
                  "1",
                  Rf2Mini.CORE_MODULE,
                  Rf2Mini.US_ENGLISH_REFSET,
                  duplicateId,
                  PREFERRED_ACCEPTABILITY));
          return extended;
        });
    return release;
  }

  /** Reports whether a description row belongs to a concept. */
  private static boolean isDescriptionOf(
      @Nonnull final String row, @Nonnull final String conceptCode) {
    return row.contains("\t" + conceptCode + "\ten\t");
  }

  /** Copies the base release into a named working directory. */
  @Nonnull
  private static Path copyOfBaseRelease(@Nonnull final String name) {
    final Path release = workDir.resolve(name);
    try (final Stream<Path> paths = Files.walk(Rf2Mini.baseRelease())) {
      for (final Path source : paths.sorted().toList()) {
        final Path target = release.resolve(Rf2Mini.baseRelease().relativize(source).toString());
        if (Files.isDirectory(source)) {
          Files.createDirectories(target);
        } else {
          Files.createDirectories(target.getParent());
          Files.copy(source, target);
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return release;
  }

  /** Rewrites the data rows of the release's description file, keeping its header first. */
  private static void rewriteDescriptionFile(
      @Nonnull final Path release, @Nonnull final RowRewriter rewriter) {
    rewriteFile(release, "sct2_Description_", rewriter);
  }

  /** Rewrites the data rows of the release's language reference set file, keeping its header. */
  private static void rewriteLanguageFile(
      @Nonnull final Path release, @Nonnull final RowRewriter rewriter) {
    rewriteFile(release, "der2_cRefset_Language", rewriter);
  }

  /**
   * Rewrites the data rows of the single file beneath a release whose name starts with a prefix.
   */
  private static void rewriteFile(
      @Nonnull final Path release,
      @Nonnull final String prefix,
      @Nonnull final RowRewriter rewriter) {
    try (final Stream<Path> paths = Files.walk(release)) {
      final Path file =
          paths
              .filter(path -> path.getFileName().toString().startsWith(prefix))
              .min(Comparator.naturalOrder())
              .orElseThrow(() -> new IllegalStateException("No file starting with " + prefix));
      final List<String> lines = Files.readAllLines(file);
      final List<String> rewritten = new ArrayList<>();
      rewritten.add(lines.get(0));
      rewritten.addAll(rewriter.rewrite(lines.subList(1, lines.size())));
      Files.write(file, rewritten);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Captures the informational log of the dialect choice made during {@code action}. */
  private static void captureDialectLog(
      @Nonnull final List<ILoggingEvent> events, @Nonnull final Executable action)
      throws Throwable {
    final Logger logger = (Logger) LoggerFactory.getLogger(DefaultDialect.class);
    final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    final Level previousLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    logger.addAppender(appender);
    try {
      action.execute();
    } finally {
      events.addAll(appender.list);
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }

  /** Returns the formatted messages of the captured events. */
  @Nonnull
  private static List<String> messages(@Nonnull final List<ILoggingEvent> events) {
    return events.stream().map(ILoggingEvent::getFormattedMessage).toList();
  }

  /** Transforms the data rows of an RF2 file. */
  @FunctionalInterface
  private interface RowRewriter {

    /**
     * Transforms the data rows.
     *
     * @param rows the rows as read, excluding the header
     * @return the rows to write
     */
    @Nonnull
    List<String> rewrite(@Nonnull List<String> rows);
  }
}
