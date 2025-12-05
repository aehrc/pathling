/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

/**
 * Represents a ping and pull bulk data import request, aligned with the SMART Bulk Data Import
 * specification.
 *
 * @param originalRequest The original request URL.
 * @param exportUrl The URL to the bulk data export endpoint (dynamic mode) or manifest file (static
 * mode).
 * @param exportType The type of export: "dynamic" to initiate a new export, or "static" to fetch a
 * pre-generated manifest.
 * @param inputSource URI for tracking the imported data throughout its lifecycle (required by SMART
 * specification).
 * @param saveMode The save mode to use throughout the entire import operation.
 * @param importFormat The expected input format (NDJSON, Parquet, or Delta).
 * @author John Grimes
 */
public record ImportPnpRequest(
    @Nonnull String originalRequest,
    @Nonnull String exportUrl,
    @Nonnull String exportType,
    @Nonnull String inputSource,
    @Nonnull SaveMode saveMode,
    @Nonnull ImportFormat importFormat
) {

}
