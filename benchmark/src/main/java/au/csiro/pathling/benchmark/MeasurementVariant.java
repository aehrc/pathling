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

import jakarta.annotation.Nonnull;
import java.util.List;

/**
 * One combination of an identifier ordering and a representation optimisation setting. The two
 * factors are crossed, giving four variants:
 *
 * <ul>
 *   <li><b>A</b> - concept code order, representation not optimised. Today's behaviour, and the
 *       baseline every other figure is stated against.
 *   <li><b>B</b> - concept code order, representation optimised. A one-line change with no
 *       trade-off, so if it captures most of the saving the reordering is unnecessary.
 *   <li><b>C</b> - depth-first pre-order, representation not optimised. Expected to change nothing,
 *       since run-length encoding only exists where it has been asked for; measured to prove that.
 *   <li><b>D</b> - depth-first pre-order, representation optimised. The proposal.
 * </ul>
 *
 * @author John Grimes
 */
public final class MeasurementVariant {

  /** The label of the baseline variant, against which every saving is stated. */
  public static final String BASELINE = "A";

  /** The label of the variant isolating the effect of optimisation alone. */
  public static final String OPTIMISED_CODE_ORDER = "B";

  /** The label of the variant isolating the effect of reordering alone. */
  public static final String UNOPTIMISED_PRE_ORDER = "C";

  /** The label of the variant combining both factors, which is the proposal. */
  public static final String PROPOSAL = "D";

  @Nonnull private final String label;
  @Nonnull private final String description;
  private final boolean preOrder;
  private final boolean optimised;

  private MeasurementVariant(
      @Nonnull final String label,
      @Nonnull final String description,
      final boolean preOrder,
      final boolean optimised) {
    this.label = label;
    this.description = description;
    this.preOrder = preOrder;
    this.optimised = optimised;
  }

  /**
   * Returns the four variants in report order.
   *
   * @return the variants
   */
  @Nonnull
  public static List<MeasurementVariant> all() {
    return List.of(
        new MeasurementVariant(BASELINE, "code order, runOptimize not called", false, false),
        new MeasurementVariant(OPTIMISED_CODE_ORDER, "code order, runOptimize called", false, true),
        new MeasurementVariant(
            UNOPTIMISED_PRE_ORDER, "pre-order, runOptimize not called", true, false),
        new MeasurementVariant(PROPOSAL, "pre-order, runOptimize called", true, true));
  }

  /**
   * Builds this variant from the loaded index. Every variant is built through the same remapping
   * code path, differing only in which permutation it applies and whether it then asks for
   * optimisation, so that no variant carries a construction artefact the others do not.
   *
   * @param source the index as loaded from the store
   * @param identity the permutation that leaves every identifier where it is
   * @param preOrderPermutation the depth-first pre-order permutation
   * @return the materialised variant
   */
  @Nonnull
  public HierarchyMaps materialise(
      @Nonnull final HierarchyMaps source,
      @Nonnull final int[] identity,
      @Nonnull final int[] preOrderPermutation) {
    final HierarchyMaps variant = source.remap(preOrder ? preOrderPermutation : identity);
    if (optimised) {
      variant.runOptimize();
    }
    return variant;
  }

  /**
   * Returns the variant's single-letter label.
   *
   * @return the label
   */
  @Nonnull
  public String getLabel() {
    return label;
  }

  /**
   * Returns a description of the variant's two factor settings.
   *
   * @return the description
   */
  @Nonnull
  public String getDescription() {
    return description;
  }

  /**
   * Returns whether this variant asked its bitmaps to optimise their representation. Only these
   * variants can contain run containers.
   *
   * @return true if optimisation was requested
   */
  public boolean isOptimised() {
    return optimised;
  }
}
