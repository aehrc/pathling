/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.view;

import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Represents a single view input for the ViewDefinition export operation.
 *
 * @param name the optional name for the output (if not provided, a name will be generated)
 * @param view the parsed FhirView to execute
 * @author John Grimes
 */
public record ViewInput(@Nullable String name, @Nonnull FhirView view) {

  /**
   * Gets the effective name for this view, using the provided name if available, otherwise falling
   * back to the view's name, or generating one from the view's resource type.
   *
   * @param index the index of this view in the request (used for disambiguation)
   * @return the effective name for this view
   */
  @Nonnull
  public String getEffectiveName(final int index) {
    if (name != null && !name.isBlank()) {
      return name;
    }
    // Use the name from the ViewDefinition if available.
    final String viewName = view.getName();
    if (viewName != null && !viewName.isBlank()) {
      return viewName;
    }
    // Generate name from resource type and index.
    return view.getResource().toLowerCase() + "_" + index;
  }
}
