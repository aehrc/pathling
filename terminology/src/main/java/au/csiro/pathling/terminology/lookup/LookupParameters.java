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

package au.csiro.pathling.terminology.lookup;

import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.TerminologyParameters;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Represents the input parameters to the lookup operation.
 *
 * @param coding the coding to look up
 * @param property the optional property or properties to look up
 * @param acceptLanguage the optional language to use for the response
 * @author John Grimes
 * @see <a
 * href="https://www.hl7.org/fhir/R4/codesystem-operation-lookup.html">CodeSystem/$lookup</a>
 */
public record LookupParameters(
    @Nonnull ImmutableCoding coding,
    @Nullable String property,
    @Nullable String acceptLanguage
) implements TerminologyParameters {

}
