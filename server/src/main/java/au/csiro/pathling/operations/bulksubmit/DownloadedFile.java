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

package au.csiro.pathling.operations.bulksubmit;

import jakarta.annotation.Nonnull;

/**
 * Represents a file that has been downloaded as part of a bulk submit manifest job.
 *
 * @param resourceType The FHIR resource type contained in the file.
 * @param fileName The name of the file (used in the $result URL).
 * @param localPath The local filesystem path where the file is stored.
 * @param manifestUrl The URL of the manifest from which this file was downloaded.
 * @author John Grimes
 */
public record DownloadedFile(
    @Nonnull String resourceType,
    @Nonnull String fileName,
    @Nonnull String localPath,
    @Nonnull String manifestUrl) {}
