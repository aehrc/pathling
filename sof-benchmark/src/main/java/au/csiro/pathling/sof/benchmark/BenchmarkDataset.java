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

package au.csiro.pathling.sof.benchmark;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * The dataset recipe portion of a benchmark file: the human-readable name, the per-size
 * populations, and the resource types the materializer is expected to keep.
 *
 * <p>These three values are exactly what {@link ManifestLocator} matches a materialized {@code
 * manifest.json} against, so the runner never reproduces the reference materializer's recipe hash.
 */
public class BenchmarkDataset {

  @Nonnull private final String name;

  @Nonnull private final Map<String, Integer> sizePopulations;

  @Nonnull private final List<String> resources;

  /**
   * Constructs a benchmark dataset descriptor.
   *
   * @param name the dataset name, as written into the manifest
   * @param sizePopulations a map of size key to the population declared for that size
   * @param resources the resource types the dataset retains
   */
  public BenchmarkDataset(
      @Nonnull final String name,
      @Nonnull final Map<String, Integer> sizePopulations,
      @Nonnull final List<String> resources) {
    this.name = name;
    this.sizePopulations = sizePopulations;
    this.resources = resources;
  }

  /**
   * Returns the dataset name.
   *
   * @return the dataset name
   */
  @Nonnull
  public String getName() {
    return name;
  }

  /**
   * Returns the resource types retained by this dataset.
   *
   * @return the resource types
   */
  @Nonnull
  public List<String> getResources() {
    return resources;
  }

  /**
   * Returns the population declared for the given size.
   *
   * @param size the size key
   * @return the declared population
   * @throws IllegalArgumentException if the size is not declared in the benchmark file
   */
  public int populationFor(@Nonnull final String size) {
    final Integer population = sizePopulations.get(size);
    if (population == null) {
      throw new IllegalArgumentException(
          "Size '" + size + "' is not declared for dataset '" + name + "'");
    }
    return population;
  }
}
