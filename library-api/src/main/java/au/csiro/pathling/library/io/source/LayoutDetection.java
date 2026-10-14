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
package au.csiro.pathling.library.io.source;

import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.Value;

/**
 * The outcome of classifying a schema by its layout, together with the fields that were the
 * evidence for it, so that a refusal can cite them.
 *
 * <p>Each marker is named by its path from the root of the schema, with array and map levels passed
 * through without notation, for example {@code name._fid}.
 *
 * @author Piotr Szul
 */
@Value
public class LayoutDetection {

  /** The layout the schema is classified as. */
  @Nonnull StorageLayout layout;

  /**
   * The fields found that only the previous layout carries, in schema order. They are cited as
   * evidence whatever the classification, and only the root versioned id among them decides it.
   */
  @Nonnull List<String> previousLayoutMarkers;

  /** The fields found that only the Parquet on FHIR layout carries, in schema order. */
  @Nonnull List<String> newLayoutMarkers;

  /**
   * Returns whether data in the detected layout is accepted.
   *
   * @return true if the layout is accepted
   */
  public boolean isSupported() {
    return layout.isSupported();
  }

  /**
   * Returns whether the schema was classified as the previous layout and also carries markers of
   * the Parquet on FHIR layout, as an append of new-layout data into a previous-layout table
   * produces. A Parquet on FHIR schema carrying a previous-layout marker that does not classify it,
   * such as the {@code _fid} of a coding a terminology function returned, is not mixed.
   *
   * @return true if the schema is the previous layout and carries markers of both layouts
   */
  public boolean isMixed() {
    return layout == StorageLayout.PREVIOUS && !newLayoutMarkers.isEmpty();
  }
}
