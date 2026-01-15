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

package au.csiro.pathling.library.io.sink;

import jakarta.validation.constraints.NotNull;

/**
 * Encapsulate information about a file that is the result of a write operation.
 *
 * @param fhirResourceType The FHIR resource type code (as a string) this file belongs to.
 * @param absoluteUrl The download url. It may be required to have a controller in between to
 *     resolve it to an actual file.
 * @author Felix Naumann
 */
public record FileInformation(@NotNull String fhirResourceType, @NotNull String absoluteUrl) {}
