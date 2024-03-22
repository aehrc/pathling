/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.export.fhir;

import java.net.URI;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * Represents a FHIR resource Reference type.
 *
 * @see <a href="https://hl7.org/fhir/r4/references.html">Resource References</a>
 */
@Value
@Builder
public class Reference {

  /**
   * The literal reverence, a relative, internal or absolute URL.
   */
  @Nullable
  String reference;

  /**
   * The type the reference refers to.
   */
  @Nullable
  String type;

  /**
   * Text alternative for the resource
   */
  @Nullable
  String display;

  /**
   * Builder for Reference instances.
   */
  @SuppressWarnings("unused")
  public static class ReferenceBuilder {

    /**
     * Sets the reference value from a URI instance.
     *
     * @param uri the URI instance.
     * @return the ReferenceBuilder instance.
     */
    ReferenceBuilder referenceFromUri(@Nonnull final URI uri) {
      return reference(uri.toString());
    }
  }

  /**
   * Creates a new Reference instance with the given literal reference.
   *
   * @param reference the reference.
   * @return a new Reference instance.
   */
  @Nonnull
  public static Reference of(@Nonnull final String reference) {
    return Reference.builder().reference(reference).build();
  }
}
