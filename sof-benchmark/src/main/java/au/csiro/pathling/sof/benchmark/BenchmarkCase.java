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
import java.util.Map;
import java.util.Optional;

/**
 * A single benchmark case: a title, the subject resource type the view runs over, the view itself
 * serialized to a JSON string ready for {@code FhirViewQuery.json(String)}, and the per-size
 * expected output row counts.
 */
public class BenchmarkCase {

  @Nonnull private final String title;

  @Nonnull private final String resource;

  @Nonnull private final String viewJson;

  @Nonnull private final Map<String, Integer> expectCount;

  /**
   * Constructs a benchmark case.
   *
   * @param title the case title
   * @param resource the subject resource type of the view
   * @param viewJson the view serialized as a JSON string
   * @param expectCount a map of size key to the expected output row count for that size
   */
  public BenchmarkCase(
      @Nonnull final String title,
      @Nonnull final String resource,
      @Nonnull final String viewJson,
      @Nonnull final Map<String, Integer> expectCount) {
    this.title = title;
    this.resource = resource;
    this.viewJson = viewJson;
    this.expectCount = expectCount;
  }

  /**
   * Returns the case title.
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
   * Returns the expected output row count for the given size, if one is declared.
   *
   * @param size the size key
   * @return the expected count, or empty when no expectation is declared for the size
   */
  @Nonnull
  public Optional<Integer> expectCountFor(@Nonnull final String size) {
    return Optional.ofNullable(expectCount.get(size));
  }
}
