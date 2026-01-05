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

import au.csiro.pathling.library.io.SaveMode;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;

/**
 * Represents a bulk data import request aligned with the SMART Bulk Data Import specification.
 *
 * @param originalRequest The original request URL.
 * @param input A map of resource type to associated directories/files to load.
 * @param saveMode The save mode to use throughout the entire import operation.
 * @param importFormat The expected input format (NDJSON, Parquet, or Delta).
 * @author Felix Naumann
 */
public record ImportRequest(
    @Nonnull String originalRequest,
    @Nonnull Map<String, Collection<String>> input,
    @Nonnull SaveMode saveMode,
    @Nonnull ImportFormat importFormat) {}
