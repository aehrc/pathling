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

package au.csiro.pathling.benchmark;

import au.csiro.pathling.terminology.local.index.ConceptDictionary;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Measures what a local terminology store's hierarchy index costs in retained heap, and attributes
 * that cost between two independent factors: how dense identifiers are assigned, and whether the
 * compressed bitmaps have been asked to adopt run-length encoding.
 *
 * <p>The harness changes no production code. It loads one store, derives a depth-first pre-order
 * from the index's own direct-edge maps, and builds the reordered and optimised variants in memory
 * alongside the original, so all four variants come from a single load and no variant store has to
 * be imported.
 *
 * <p>Configured by system properties:
 *
 * <ul>
 *   <li>{@code pathling.benchmark.terminology.storagePath} - the store to measure (required).
 *   <li>{@code pathling.benchmark.terminology.systemVersionId} - the code system version within it
 *       (required).
 *   <li>{@code pathling.benchmark.hierarchy.repeats} - runs per variant, default 3.
 *   <li>{@code pathling.benchmark.hierarchy.statusQuoDriverHeapGb} - optional. The minimum viable
 *       driver heap measured for the status quo, in gigabytes. Supplying it and its counterpart
 *       lets the harness evaluate the second sub-rule of the decision rule, which the fine-grained
 *       measurement cannot reach on its own.
 *   <li>{@code pathling.benchmark.hierarchy.winnerDriverHeapGb} - optional. The same figure for the
 *       winning variant.
 * </ul>
 *
 * @author John Grimes
 */
public final class HierarchyIndexMemoryHarness {

  private static final Logger log = LoggerFactory.getLogger(HierarchyIndexMemoryHarness.class);

  /** System property naming the terminology store to measure. */
  static final String STORAGE_PATH_PROPERTY = TerminologyBenchmarkState.STORAGE_PATH_PROPERTY;

  /** System property naming the code system version within the store. */
  static final String SYSTEM_VERSION_ID_PROPERTY = "pathling.benchmark.terminology.systemVersionId";

  /** System property naming the number of measurement runs per variant. */
  static final String REPEATS_PROPERTY = "pathling.benchmark.hierarchy.repeats";

  /** System property carrying the separately measured status quo driver heap, in gigabytes. */
  static final String STATUS_QUO_DRIVER_HEAP_PROPERTY =
      "pathling.benchmark.hierarchy.statusQuoDriverHeapGb";

  /** System property carrying the separately measured winning variant driver heap, in gigabytes. */
  static final String WINNER_DRIVER_HEAP_PROPERTY =
      "pathling.benchmark.hierarchy.winnerDriverHeapGb";

  /** The default number of measurement runs per variant. */
  private static final int DEFAULT_REPEATS = 3;

  /**
   * The proportion of variant B's total retained heap that variant D must save to earn the loss of
   * identifier stability across releases. Fixed in the plan before any figure was taken.
   */
  private static final double REORDERING_THRESHOLD = 0.25;

  /** The granularity of the original driver heap measurement, in gigabytes. */
  private static final double DRIVER_HEAP_STEP_GB = 0.5;

  private HierarchyIndexMemoryHarness() {
    // Runnable harness.
  }

  /**
   * Runs the measurement and emits the report.
   *
   * @param args ignored; the harness is configured by system properties
   */
  public static void main(@Nonnull final String[] args) {
    final String storagePath = requiredProperty(STORAGE_PATH_PROPERTY);
    final String systemVersionId = requiredProperty(SYSTEM_VERSION_ID_PROPERTY);
    final int repeats = repeats();

    log.info("Opening terminology store at {}", storagePath);
    final TerminologyStoreReader reader = TerminologyStoreReader.open(storagePath, Map.of());

    log.info("Loading the concept dictionary for {}", systemVersionId);
    final ConceptDictionary dictionary = ConceptDictionary.load(reader, systemVersionId);
    final int conceptCount = dictionary.size();
    log.info("Dictionary holds {} concepts", conceptCount);

    log.info("Loading the hierarchy index");
    final HierarchyMaps source = HierarchyMaps.load(reader, systemVersionId);

    // Reported so that a reader can confirm the remapping introduces no size artefact of its own:
    // variant A is built by remapping through the identity, and should match this figure.
    final long asLoadedBytes = HierarchyVariantFootprint.measure(source).getTotalRetainedBytes();
    log.info("Index as loaded from the store retains {} bytes", grouped(asLoadedBytes));

    log.info("Deriving the depth-first pre-order from the index's direct edges");
    final int[] identity = DenseIdOrdering.identity(conceptCount);
    final int[] preOrder = DenseIdOrdering.preOrder(source, conceptCount);

    final List<MeasurementVariant> variants = MeasurementVariant.all();
    final Map<String, List<HierarchyVariantFootprint>> results = new LinkedHashMap<>();
    for (final MeasurementVariant variant : variants) {
      final List<HierarchyVariantFootprint> footprints = new ArrayList<>();
      for (int repeat = 1; repeat <= repeats; repeat++) {
        log.info(
            "Measuring variant {} ({}), run {} of {}",
            variant.getLabel(),
            variant.getDescription(),
            repeat,
            repeats);
        // The variant becomes unreachable at the end of each iteration, so the harness holds the
        // source index and at most one variant at a time.
        final HierarchyMaps materialised = variant.materialise(source, identity, preOrder);
        footprints.add(HierarchyVariantFootprint.measure(materialised));
      }
      results.put(variant.getLabel(), footprints);
    }

    report(storagePath, systemVersionId, conceptCount, asLoadedBytes, repeats, variants, results);
  }

  /**
   * Emits the whole report: a per-variant table, the attribution summary, the direction breakdown,
   * the reproducibility statement, and the decision rule evaluated against the figures.
   *
   * @param storagePath the store measured
   * @param systemVersionId the code system version measured
   * @param conceptCount the size of the concept dictionary
   * @param asLoadedBytes the retained heap of the index exactly as loaded
   * @param repeats the number of runs per variant
   * @param variants the variants measured, in report order
   * @param results each variant's footprints, one per run
   */
  private static void report(
      @Nonnull final String storagePath,
      @Nonnull final String systemVersionId,
      final int conceptCount,
      final long asLoadedBytes,
      final int repeats,
      @Nonnull final List<MeasurementVariant> variants,
      @Nonnull final Map<String, List<HierarchyVariantFootprint>> results) {
    log.info("");
    log.info("Hierarchy index memory measurement");
    log.info("  store             {}", storagePath);
    log.info("  code system       {}", systemVersionId);
    log.info("  concepts          {}", grouped(conceptCount));
    log.info("  runs per variant  {}", repeats);
    log.info("  index as loaded   {} bytes retained", grouped(asLoadedBytes));

    for (final MeasurementVariant variant : variants) {
      reportVariant(variant, results.get(variant.getLabel()).get(0));
    }
    reportAttribution(variants, results);
    reportDirections(variants, results);
    reportReproducibility(repeats, variants, results);
    reportDecision(variants, results);
  }

  /**
   * Emits one variant's per-map table.
   *
   * @param variant the variant described
   * @param footprint that variant's first run
   */
  private static void reportVariant(
      @Nonnull final MeasurementVariant variant,
      @Nonnull final HierarchyVariantFootprint footprint) {
    log.info("");
    log.info("Variant {} - {}", variant.getLabel(), variant.getDescription());
    log.info(
        String.format(
            "  %-12s %14s %18s %12s %12s %12s",
            "map", "entries", "retained bytes", "array", "bitmap", "run"));
    for (final HierarchyMapFootprint map : footprint.getMaps()) {
      log.info(
          String.format(
              "  %-12s %14s %18s %12s %12s %12s",
              map.getName(),
              grouped(map.getEntries()),
              grouped(map.getRetainedBytes()),
              grouped(map.getArrayContainers()),
              grouped(map.getBitmapContainers()),
              grouped(map.getRunContainers())));
    }
    log.info(
        String.format(
            "  %-12s %14s %18s %12s %12s %12s",
            "total",
            "-",
            grouped(footprint.getTotalRetainedBytes()),
            grouped(footprint.getTotalArrayContainers()),
            grouped(footprint.getTotalBitmapContainers()),
            grouped(footprint.getTotalRunContainers())));
    if (!variant.isOptimised() && footprint.getTotalRunContainers() != 0) {
      log.error(
          "Variant {} did not request optimisation but holds {} run containers; the measurement is"
              + " not measuring what it claims",
          variant.getLabel(),
          footprint.getTotalRunContainers());
    }
  }

  /**
   * Emits the attribution summary, stating each variant's total against the baseline and the
   * proposal against the change that costs nothing.
   *
   * @param variants the variants measured
   * @param results each variant's footprints
   */
  private static void reportAttribution(
      @Nonnull final List<MeasurementVariant> variants,
      @Nonnull final Map<String, List<HierarchyVariantFootprint>> results) {
    final long baseline = total(results, MeasurementVariant.BASELINE);
    final long optimisedCodeOrder = total(results, MeasurementVariant.OPTIMISED_CODE_ORDER);
    final long unoptimisedPreOrder = total(results, MeasurementVariant.UNOPTIMISED_PRE_ORDER);
    log.info("");
    log.info("Attribution (total retained bytes)");
    for (final MeasurementVariant variant : variants) {
      final long value = total(results, variant.getLabel());
      log.info(
          String.format(
              "  %-2s %-34s %18s   %s",
              variant.getLabel(),
              variant.getDescription(),
              grouped(value),
              attribution(
                  variant.getLabel(), baseline, optimisedCodeOrder, unoptimisedPreOrder, value)));
    }
  }

  /**
   * Describes one variant's total against the comparisons that isolate a factor. The proposal is
   * stated against both of the single-factor variants, so that what the reordering contributes over
   * the change that costs nothing, and what optimisation contributes on top of the reordering, can
   * each be read off directly.
   *
   * @param label the variant being described
   * @param baseline variant A's total
   * @param optimisedCodeOrder variant B's total
   * @param unoptimisedPreOrder variant C's total
   * @param value this variant's total
   * @return the comparison text
   */
  @Nonnull
  private static String attribution(
      @Nonnull final String label,
      final long baseline,
      final long optimisedCodeOrder,
      final long unoptimisedPreOrder,
      final long value) {
    if (MeasurementVariant.BASELINE.equals(label)) {
      return "baseline";
    }
    if (MeasurementVariant.PROPOSAL.equals(label)) {
      return String.format(
          "%s vs A, %s vs B, %s vs C",
          percentChange(baseline, value),
          percentChange(optimisedCodeOrder, value),
          percentChange(unoptimisedPreOrder, value));
    }
    return String.format("%s vs A", percentChange(baseline, value));
  }

  /**
   * Emits the direction breakdown, which tests the proposal's own prediction that the descendant
   * maps shrink sharply while the ancestor maps stay roughly flat. One linear order cannot make
   * both directions contiguous, so a single total would hide a loss on one side behind a gain on
   * the other.
   *
   * @param variants the variants measured
   * @param results each variant's footprints
   */
  private static void reportDirections(
      @Nonnull final List<MeasurementVariant> variants,
      @Nonnull final Map<String, List<HierarchyVariantFootprint>> results) {
    log.info("");
    log.info("Direction breakdown (retained bytes, and change against variant A)");
    for (final String direction : List.of(HierarchyMaps.DESCENDANTS, HierarchyMaps.ANCESTORS)) {
      final long baseline =
          results.get(MeasurementVariant.BASELINE).get(0).getMap(direction).getRetainedBytes();
      final StringBuilder line = new StringBuilder(String.format("  %-12s", direction));
      for (final MeasurementVariant variant : variants) {
        final long value =
            results.get(variant.getLabel()).get(0).getMap(direction).getRetainedBytes();
        line.append(
            String.format(
                "  %s %18s (%s)",
                variant.getLabel(), grouped(value), percentChange(baseline, value)));
      }
      log.info(line.toString());
    }
  }

  /**
   * Emits the reproducibility statement. A difference smaller than the instrument's own spread
   * cannot be read as a result, so the report states both.
   *
   * @param repeats the number of runs per variant
   * @param variants the variants measured
   * @param results each variant's footprints
   */
  private static void reportReproducibility(
      final int repeats,
      @Nonnull final List<MeasurementVariant> variants,
      @Nonnull final Map<String, List<HierarchyVariantFootprint>> results) {
    log.info("");
    log.info("Reproducibility over {} runs, per variant", repeats);
    log.info(String.format("  %-8s %18s %18s %10s", "variant", "min bytes", "max bytes", "spread"));
    for (final MeasurementVariant variant : variants) {
      log.info(
          String.format(
              "  %-8s %18s %18s %9.2f%%",
              variant.getLabel(),
              grouped(minimum(results, variant.getLabel())),
              grouped(maximum(results, variant.getLabel())),
              spread(results, variant.getLabel())));
    }
  }

  /**
   * Emits the decision rule, restated and evaluated against the figures. Both sub-rules are
   * reported even when the first settles the question, so the record is complete.
   *
   * @param variants the variants measured
   * @param results each variant's footprints
   */
  private static void reportDecision(
      @Nonnull final List<MeasurementVariant> variants,
      @Nonnull final Map<String, List<HierarchyVariantFootprint>> results) {
    final long baseline = total(results, MeasurementVariant.BASELINE);
    final long optimisedCodeOrder = total(results, MeasurementVariant.OPTIMISED_CODE_ORDER);
    final long proposal = total(results, MeasurementVariant.PROPOSAL);
    final double reorderingSaving = saving(optimisedCodeOrder, proposal);
    final boolean subRuleA = reorderingSaving >= REORDERING_THRESHOLD;

    final Double statusQuoHeap = optionalDoubleProperty(STATUS_QUO_DRIVER_HEAP_PROPERTY);
    final Double winnerHeap = optionalDoubleProperty(WINNER_DRIVER_HEAP_PROPERTY);
    final String subRuleB;
    final boolean driverHeapRuleMet;
    if (statusQuoHeap == null || winnerHeap == null) {
      subRuleB =
          "not determined - supply -D"
              + STATUS_QUO_DRIVER_HEAP_PROPERTY
              + " and -D"
              + WINNER_DRIVER_HEAP_PROPERTY
              + " from the driver heap search";
      driverHeapRuleMet = false;
    } else {
      driverHeapRuleMet = statusQuoHeap - winnerHeap >= DRIVER_HEAP_STEP_GB;
      subRuleB =
          String.format(
              "%s (%.1f GB to %.1f GB)",
              driverHeapRuleMet ? "met" : "not met", statusQuoHeap, winnerHeap);
    }

    // Optimisation is worth adopting instead of the reordering only if it captures a material share
    // of what the two factors together achieve. The share is measured against the same threshold
    // the
    // plan fixed for the reordering, because that is the only figure fixed before the measurement.
    final long unoptimisedPreOrder = total(results, MeasurementVariant.UNOPTIMISED_PRE_ORDER);
    final double optimisationSaving = saving(baseline, optimisedCodeOrder);
    final double combinedSaving = saving(baseline, proposal);
    final double capturedShare = combinedSaving == 0 ? 0.0 : optimisationSaving / combinedSaving;
    final boolean adoptOptimisationAlone = capturedShare >= REORDERING_THRESHOLD;

    // Once the reordering is being made, optimisation costs nothing more, so any saving the
    // instrument can resolve justifies it.
    final double furtherSaving = saving(unoptimisedPreOrder, proposal);
    final double instrumentFloor =
        Math.max(
                spread(results, MeasurementVariant.UNOPTIMISED_PRE_ORDER),
                spread(results, MeasurementVariant.PROPOSAL))
            / 100.0;
    final boolean adoptOptimisationAsWell = furtherSaving > instrumentFloor;

    log.info("");
    log.info("Decision rule");
    log.info(
        String.format(
            "  (a) D reduces total retained heap by >= %.0f%% against B      : %s (%+.1f%%)",
            REORDERING_THRESHOLD * 100, subRuleA ? "met" : "not met", -reorderingSaving * 100));
    log.info("  (b) D lowers the UK edition's minimum viable driver heap");
    log.info(
        String.format(
            "      by >= one %.1f GB step against A                        : %s",
            DRIVER_HEAP_STEP_GB, subRuleB));
    log.info(
        "  Adopt reordering: {}",
        adoptReordering(subRuleA, driverHeapRuleMet, statusQuoHeap != null));
    log.info(
        String.format(
            "  Adopt runOptimize alone, in place of the reordering: %s"
                + " (%+.1f%% vs A, which is %.1f%% of what A to D achieves)",
            adoptOptimisationAlone ? "yes" : "no", -optimisationSaving * 100, capturedShare * 100));
    log.info(
        String.format(
            "  Adopt runOptimize as well as the reordering: %s (%+.1f%% vs C, instrument spread"
                + " %.2f%%)",
            adoptOptimisationAsWell ? "yes" : "no", -furtherSaving * 100, instrumentFloor * 100));
    log.info("");
    log.info(
        "The four variants measured were: {}",
        variants.stream()
            .map(variant -> variant.getLabel() + " (" + variant.getDescription() + ")")
            .toList());
  }

  @Nonnull
  private static String adoptReordering(
      final boolean subRuleA, final boolean driverHeapRuleMet, final boolean driverHeapKnown) {
    if (subRuleA || driverHeapRuleMet) {
      return "yes";
    }
    return driverHeapKnown
        ? "no"
        : "no on the measured evidence; sub-rule (b) is still to be determined";
  }

  private static long total(
      @Nonnull final Map<String, List<HierarchyVariantFootprint>> results,
      @Nonnull final String label) {
    return results.get(label).get(0).getTotalRetainedBytes();
  }

  private static long minimum(
      @Nonnull final Map<String, List<HierarchyVariantFootprint>> results,
      @Nonnull final String label) {
    return results.get(label).stream()
        .mapToLong(HierarchyVariantFootprint::getTotalRetainedBytes)
        .min()
        .orElseThrow();
  }

  private static long maximum(
      @Nonnull final Map<String, List<HierarchyVariantFootprint>> results,
      @Nonnull final String label) {
    return results.get(label).stream()
        .mapToLong(HierarchyVariantFootprint::getTotalRetainedBytes)
        .max()
        .orElseThrow();
  }

  /**
   * Returns a variant's spread across runs, as a percentage of its smallest run.
   *
   * @param results each variant's footprints
   * @param label the variant to summarise
   * @return the spread, as a percentage
   */
  private static double spread(
      @Nonnull final Map<String, List<HierarchyVariantFootprint>> results,
      @Nonnull final String label) {
    final long min = minimum(results, label);
    final long max = maximum(results, label);
    return min == 0 ? 0.0 : (max - min) * 100.0 / min;
  }

  /**
   * Returns the proportion of a baseline that a value saves, negative if it costs more.
   *
   * @param baseline the figure compared against
   * @param value the figure compared
   * @return the saving as a proportion of the baseline
   */
  private static double saving(final long baseline, final long value) {
    return baseline == 0 ? 0.0 : (baseline - value) / (double) baseline;
  }

  @Nonnull
  private static String percentChange(final long baseline, final long value) {
    return baseline == 0 ? "n/a" : String.format("%+.1f%%", (value - baseline) * 100.0 / baseline);
  }

  @Nonnull
  private static String grouped(final long value) {
    return String.format("%,d", value);
  }

  private static int repeats() {
    final String value = System.getProperty(REPEATS_PROPERTY);
    if (value == null || value.isBlank()) {
      return DEFAULT_REPEATS;
    }
    final int parsed = Integer.parseInt(value.trim());
    if (parsed < 1) {
      throw new IllegalArgumentException(
          "Property " + REPEATS_PROPERTY + " must be at least 1, but was " + parsed);
    }
    return parsed;
  }

  @Nullable
  private static Double optionalDoubleProperty(@Nonnull final String name) {
    final String value = System.getProperty(name);
    return value == null || value.isBlank() ? null : Double.valueOf(value.trim());
  }

  @Nonnull
  private static String requiredProperty(@Nonnull final String name) {
    final String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(
          "Required harness system property is not set: -D" + name + "=<value>");
    }
    return value;
  }
}
