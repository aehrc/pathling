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

package au.csiro.pathling.operations.bulkimport;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;

/**
 * Represents a SMART Bulk Data Import manifest request body.
 *
 * @param inputFormat the format of the input data
 * @param inputSource the source of the input data
 * @param input the list of input items
 * @param saveMode the import mode
 * @author John Grimes
 */
public record ImportManifest(
    @Nonnull @JsonProperty("inputFormat") String inputFormat,
    @Nullable @JsonProperty("inputSource") String inputSource,
    @Nonnull @JsonProperty("input") List<ImportManifestInput> input,
    @Nullable @JsonProperty("saveMode") String saveMode) {}
