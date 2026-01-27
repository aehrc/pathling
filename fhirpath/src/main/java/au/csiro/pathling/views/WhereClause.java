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

package au.csiro.pathling.views;

import com.google.gson.annotations.SerializedName;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 'where' filters are FHIRPath expressions joined with an implicit "and". This enables users to
 * select a subset of rows that match a specific need. For example, a user may be interested only in
 * a subset of observations based on code value and can filter them here.
 *
 * @author John Grimes
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WhereClause {

  /** The FHIRPath expression for the filter. */
  @NotNull
  @SerializedName("path")
  String expression;

  /** An optional human-readable description of the filter. */
  @Nullable String description;
}
