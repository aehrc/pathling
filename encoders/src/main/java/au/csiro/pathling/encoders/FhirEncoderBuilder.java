/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
 *
 */

package au.csiro.pathling.encoders;

import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

/**
 * Encoder builder. Specifies FHIR version and other parameters affecting encoder functionality,
 * such as max nesting level for recursive types with the fluent API.
 */
public class FhirEncoderBuilder {

  private static final boolean DEFAULT_ENABLE_EXTENSIONS = false;
  private static final int DEFAULT_MAX_NESTING_LEVEL = 0;

  @Nonnull
  private final FhirVersionEnum fhirVersion;

  private int maxNestingLevel;

  @Nonnull
  private Set<String> openTypes;

  private boolean enableExtensions;

  FhirEncoderBuilder(@Nonnull final FhirVersionEnum fhirVersion) {
    this.fhirVersion = fhirVersion;
    this.maxNestingLevel = DEFAULT_MAX_NESTING_LEVEL;
    this.openTypes = Collections.emptySet();
    this.enableExtensions = DEFAULT_ENABLE_EXTENSIONS;
  }

  /**
   * Set the maximum nesting level for recursive data types.
   *
   * @param maxNestingLevel the maximum nesting level
   * @return this builder
   */
  public FhirEncoderBuilder withMaxNestingLevel(final int maxNestingLevel) {
    this.maxNestingLevel = maxNestingLevel;
    return this;
  }

  /**
   * Sets the list of types that are encoded within open types, such as extensions.
   *
   * @param openTypes the list of types
   * @return this builder
   */
  public FhirEncoderBuilder withOpenTypes(final Set<String> openTypes) {
    this.openTypes = openTypes;
    return this;
  }

  /**
   * Sets the list of all types to be encoded for open types, such as extensions.
   *
   * @return this builder
   */
  public FhirEncoderBuilder withAllOpenTypes() {
    return withOpenTypes(FhirEncoders.ALL_OPEN_TYPES);
  }

  /**
   * Switches on/off the support for extensions in encoders.
   *
   * @param enable if extensions should be enabled.
   * @return this builder
   */
  public FhirEncoderBuilder withExtensionsEnabled(final boolean enable) {
    this.enableExtensions = enable;
    return this;
  }

  /**
   * Get or create an {@link FhirEncoders} instance that matches the builder's configuration.
   *
   * @return an Encoders instance.
   */
  public FhirEncoders getOrCreate() {
    return FhirEncoders.getOrCreate(fhirVersion, maxNestingLevel, openTypes, enableExtensions);
  }
}
