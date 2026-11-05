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

package au.csiro.pathling.terminology.store;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import lombok.Value;

/**
 * The result of the pre-scan of a FHIR terminology source: the resources it holds, the digests of
 * its bytes when it is a single file, and the identity of the FHIR NPM package it is, if it is one.
 *
 * @author John Grimes
 */
@Value
public class FhirSourceScan {

  /** The scanned resources, in the order they were encountered. */
  @Nonnull List<ScannedResource> resources;

  /** The SHA-1 of the source file bytes as lowercase hexadecimal, or null for a directory. */
  @Nullable String sha1;

  /** The SHA-256 of the source file bytes as lowercase hexadecimal, or null for a directory. */
  @Nullable String sha256;

  /** The {@code name} from the package's {@code package.json}, or null if there is none. */
  @Nullable String packageName;

  /** The {@code version} from the package's {@code package.json}, or null if there is none. */
  @Nullable String packageVersion;

  /** Whether the source is a FHIR NPM package. */
  boolean isPackage;
}
