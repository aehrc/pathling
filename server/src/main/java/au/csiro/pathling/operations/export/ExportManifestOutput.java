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

package au.csiro.pathling.operations.export;

import jakarta.annotation.Nonnull;
import java.util.List;

/**
 * One finished export unit within a completion manifest: a friendly name and one or more
 * downloadable file locations (local file URLs that the manifest builder maps to {@code $result}
 * download URLs). Shared by both the {@code $viewdefinition-export} and {@code $sqlquery-export}
 * operations, which produce the identical manifest shape (one output per exported unit).
 *
 * @param name the output name (one per exported unit)
 * @param fileUrls the local file URLs for this output, in order; repeats once per partitioned file
 * @author John Grimes
 */
public record ExportManifestOutput(@Nonnull String name, @Nonnull List<String> fileUrls) {}
