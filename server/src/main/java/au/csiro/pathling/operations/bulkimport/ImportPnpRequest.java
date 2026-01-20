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
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Represents a ping and pull bulk data import request, aligned with the SMART Bulk Data Import
 * specification.
 *
 * @param originalRequest The original request URL.
 * @param exportUrl The URL to the bulk data export endpoint (dynamic mode) or manifest file (static
 *     mode).
 * @param exportType The type of export: "dynamic" to initiate a new export, or "static" to fetch a
 *     pre-generated manifest.
 * @param saveMode The save mode to use throughout the entire import operation.
 * @param importFormat The expected input format (NDJSON, Parquet, or Delta).
 * @param types Resource types to include in the export (_type parameter).
 * @param since Export resources modified after this time (_since parameter).
 * @param until Export resources modified before this time (_until parameter).
 * @param elements Elements to include in the export (_elements parameter).
 * @param typeFilters FHIR search queries to filter resources (_typeFilter parameter).
 * @param includeAssociatedData Associated data to include (includeAssociatedData parameter).
 * @author John Grimes
 */
public record ImportPnpRequest(
    @Nonnull String originalRequest,
    @Nonnull String exportUrl,
    @Nonnull String exportType,
    @Nonnull SaveMode saveMode,
    @Nonnull ImportFormat importFormat,
    @Nonnull List<String> types,
    @Nonnull Optional<Instant> since,
    @Nonnull Optional<Instant> until,
    @Nonnull List<String> elements,
    @Nonnull List<String> typeFilters,
    @Nonnull List<String> includeAssociatedData) {}
