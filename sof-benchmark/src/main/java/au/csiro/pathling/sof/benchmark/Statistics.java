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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import java.util.List;

/**
 * Computes the fixed contract-v2 statistics set — exactly {@code mean}, {@code stddev}, {@code
 * min}, {@code max} and {@code median} — over a case's raw timing samples.
 *
 * <p>The sample standard deviation (n-1 denominator) is used, matching a JMH primary-metric
 * projection; richer percentiles and a confidence interval are intentionally left to a consumer to
 * recompute from the raw {@code samplesMs}, which remain in the report.
 */
public final class Statistics {

  private Statistics() {}

  /**
   * Builds the {@code stats} object node for a set of timing samples.
   *
   * @param mapper the object mapper used to create the node
   * @param samples the raw timing samples in milliseconds
   * @return an object node with exactly {@code mean}, {@code stddev}, {@code min}, {@code max} and
   *     {@code median}; empty when there are no samples
   */
  @Nonnull
  public static ObjectNode toNode(
      @Nonnull final ObjectMapper mapper, @Nonnull final List<Double> samples) {
    final ObjectNode stats = mapper.createObjectNode();
    if (samples.isEmpty()) {
      return stats;
    }
    final List<Double> sorted = samples.stream().sorted().toList();
    final double mean = sorted.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    stats.put("mean", mean);
    stats.put("stddev", stddev(sorted, mean));
    stats.put("min", sorted.get(0));
    stats.put("max", sorted.get(sorted.size() - 1));
    stats.put("median", median(sorted));
    return stats;
  }

  private static double stddev(@Nonnull final List<Double> values, final double mean) {
    if (values.size() < 2) {
      return 0.0;
    }
    final double sumSquaredDeviations =
        values.stream().mapToDouble(value -> (value - mean) * (value - mean)).sum();
    return Math.sqrt(sumSquaredDeviations / (values.size() - 1));
  }

  private static double median(@Nonnull final List<Double> sorted) {
    final int size = sorted.size();
    final int mid = size / 2;
    if (size % 2 == 0) {
      return (sorted.get(mid - 1) + sorted.get(mid)) / 2.0;
    }
    return sorted.get(mid);
  }
}
