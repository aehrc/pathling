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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * What one measurement variant - a combination of an identifier ordering and a representation
 * optimisation setting - occupies, broken down by map.
 *
 * @author John Grimes
 */
public final class HierarchyVariantFootprint {

  @Nonnull private final List<HierarchyMapFootprint> maps;

  private HierarchyVariantFootprint(@Nonnull final List<HierarchyMapFootprint> maps) {
    this.maps = maps;
  }

  /**
   * Measures every map of a variant.
   *
   * @param variant the four maps to measure
   * @return the variant's footprint
   */
  @Nonnull
  public static HierarchyVariantFootprint measure(@Nonnull final HierarchyMaps variant) {
    final List<HierarchyMapFootprint> measured = new ArrayList<>();
    variant.byName().forEach((name, map) -> measured.add(HierarchyMapFootprint.measure(name, map)));
    return new HierarchyVariantFootprint(Collections.unmodifiableList(measured));
  }

  /**
   * Returns the per-map footprints in report order.
   *
   * @return the map footprints
   */
  @Nonnull
  public List<HierarchyMapFootprint> getMaps() {
    return maps;
  }

  /**
   * Returns the footprint of one named map.
   *
   * @param name the map name, one of the constants on {@link HierarchyMaps}
   * @return that map's footprint
   * @throws IllegalArgumentException if the variant has no map of that name
   */
  @Nonnull
  public HierarchyMapFootprint getMap(@Nonnull final String name) {
    return maps.stream()
        .filter(map -> name.equals(map.getName()))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("No such map in the measurement: " + name));
  }

  /**
   * Returns the total retained heap of all four maps, which is the figure the decision rule is
   * evaluated against.
   *
   * @return the total retained size in bytes
   */
  public long getTotalRetainedBytes() {
    return maps.stream().mapToLong(HierarchyMapFootprint::getRetainedBytes).sum();
  }

  /**
   * Returns the total number of array containers across all four maps.
   *
   * @return the array container count
   */
  public long getTotalArrayContainers() {
    return maps.stream().mapToLong(HierarchyMapFootprint::getArrayContainers).sum();
  }

  /**
   * Returns the total number of bitmap containers across all four maps.
   *
   * @return the bitmap container count
   */
  public long getTotalBitmapContainers() {
    return maps.stream().mapToLong(HierarchyMapFootprint::getBitmapContainers).sum();
  }

  /**
   * Returns the total number of run containers across all four maps.
   *
   * @return the run container count
   */
  public long getTotalRunContainers() {
    return maps.stream().mapToLong(HierarchyMapFootprint::getRunContainers).sum();
  }
}
