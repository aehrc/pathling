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

package au.csiro.pathling.operations.sql;

import jakarta.annotation.Nonnull;

/**
 * One media type parsed from an HTTP {@code Accept} header, with its quality value, used to rank a
 * client's format preferences during content negotiation.
 *
 * @param type the lower-cased media type, with any parameters stripped
 * @param quality the {@code q} parameter, defaulting to 1.0 when absent or unparseable
 * @author John Grimes
 */
public record AcceptEntry(@Nonnull String type, double quality) {}
