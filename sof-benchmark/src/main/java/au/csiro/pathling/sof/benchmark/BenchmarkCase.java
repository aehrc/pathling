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

/**
 * A single benchmark case: its stable {@code id}, a free-text title, the subject resource type the
 * view runs over, the view serialized to a JSON string ready for {@code
 * FhirViewQuery.json(String)}, and whether count variance is permitted.
 *
 * <p>Under contract v2 the expected output row counts live in the sibling checkfile (keyed by
 * {@code id}), not on the case. When {@code countVariancePermitted} is true, the case's reference
 * is resolved in {@code where}/{@code forEach} position where empty-vs-null-vs-error is
 * engine-specific, so the runner must not flag a cross-engine count divergence as {@code
 * count_mismatch}.
 */
public class BenchmarkCase {

  @Nonnull private final String id;

  @Nonnull private final String title;

  @Nonnull private final String resource;

  @Nonnull private final String viewJson;

  private final boolean countVariancePermitted;

  /**
   * Constructs a benchmark case.
   *
   * @param id the stable case id (the checkfile assertion key and the report's per-case key)
   * @param title the free-text case title
   * @param resource the subject resource type of the view
   * @param viewJson the view serialized as a JSON string
   * @param countVariancePermitted whether a cross-engine count divergence must not be flagged
   */
  public BenchmarkCase(
      @Nonnull final String id,
      @Nonnull final String title,
      @Nonnull final String resource,
      @Nonnull final String viewJson,
      final boolean countVariancePermitted) {
    this.id = id;
    this.title = title;
    this.resource = resource;
    this.viewJson = viewJson;
    this.countVariancePermitted = countVariancePermitted;
  }

  /**
   * Returns the stable case id.
   *
   * @return the case id
   */
  @Nonnull
  public String getId() {
    return id;
  }

  /**
   * Returns the free-text case title.
   *
   * @return the case title
   */
  @Nonnull
  public String getTitle() {
    return title;
  }

  /**
   * Returns the subject resource type of the view.
   *
   * @return the subject resource type
   */
  @Nonnull
  public String getResource() {
    return resource;
  }

  /**
   * Returns the view serialized as a JSON string, suitable for {@code FhirViewQuery.json(String)}.
   *
   * @return the view JSON
   */
  @Nonnull
  public String getViewJson() {
    return viewJson;
  }

  /**
   * Returns whether a cross-engine count divergence must not be flagged as {@code count_mismatch}
   * for this case.
   *
   * @return true when count variance is permitted
   */
  public boolean isCountVariancePermitted() {
    return countVariancePermitted;
  }
}
