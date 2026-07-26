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
import java.util.Map;
import org.openjdk.jol.info.GraphLayout;
import org.roaringbitmap.ContainerPointer;
import org.roaringbitmap.RoaringBitmap;

/**
 * What one map of a hierarchy index occupies, and what it is built from: the true retained heap of
 * its object graph, and a count of its compressed chunks by kind.
 *
 * <p>The retained figure comes from walking the object graph rather than from the bitmap library's
 * own estimate, which its documentation rules out for sparse data spanning a wide range of values -
 * exactly the shape a descendant set takes when identifiers are assigned in concept code order. The
 * chunk counts test the mechanism rather than only the outcome: if reordering works, run counts
 * rise and array and bitmap counts fall. Bytes falling with an unchanged histogram would mean
 * something other than the stated hypothesis is responsible.
 *
 * @author John Grimes
 */
public final class HierarchyMapFootprint {

  @Nonnull private final String name;
  private final int entries;
  private final long retainedBytes;
  private final long arrayContainers;
  private final long bitmapContainers;
  private final long runContainers;

  private HierarchyMapFootprint(
      @Nonnull final String name,
      final int entries,
      final long retainedBytes,
      final long arrayContainers,
      final long bitmapContainers,
      final long runContainers) {
    this.name = name;
    this.entries = entries;
    this.retainedBytes = retainedBytes;
    this.arrayContainers = arrayContainers;
    this.bitmapContainers = bitmapContainers;
    this.runContainers = runContainers;
  }

  /**
   * Measures one map of a hierarchy index.
   *
   * @param name the map's name, for reporting
   * @param map the map to measure
   * @return the map's footprint
   */
  @Nonnull
  public static HierarchyMapFootprint measure(
      @Nonnull final String name, @Nonnull final Map<Integer, RoaringBitmap> map) {
    long arrays = 0;
    long bitmaps = 0;
    long runs = 0;
    for (final RoaringBitmap bitmap : map.values()) {
      final ContainerPointer pointer = bitmap.getContainerPointer();
      // The pointer returns a null container once it has passed the last one.
      while (pointer.getContainer() != null) {
        if (pointer.isRunContainer()) {
          runs++;
        } else if (pointer.isBitmapContainer()) {
          bitmaps++;
        } else {
          arrays++;
        }
        pointer.advance();
      }
    }
    return new HierarchyMapFootprint(
        name, map.size(), GraphLayout.parseInstance(map).totalSize(), arrays, bitmaps, runs);
  }

  /**
   * Returns the map's name.
   *
   * @return the name
   */
  @Nonnull
  public String getName() {
    return name;
  }

  /**
   * Returns the number of keys in the map.
   *
   * @return the entry count
   */
  public int getEntries() {
    return entries;
  }

  /**
   * Returns the true retained heap of the map's object graph.
   *
   * @return the retained size in bytes
   */
  public long getRetainedBytes() {
    return retainedBytes;
  }

  /**
   * Returns the number of array containers across the map's bitmaps.
   *
   * @return the array container count
   */
  public long getArrayContainers() {
    return arrayContainers;
  }

  /**
   * Returns the number of bitmap containers across the map's bitmaps.
   *
   * @return the bitmap container count
   */
  public long getBitmapContainers() {
    return bitmapContainers;
  }

  /**
   * Returns the number of run-length-encoded containers across the map's bitmaps. This is zero for
   * any map whose bitmaps have not been asked to optimise their representation.
   *
   * @return the run container count
   */
  public long getRunContainers() {
    return runContainers;
  }
}
