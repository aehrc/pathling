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

import java.time.Instant;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Value;

/**
 * Represents a FHIR resource Parameters type.
 *
 * @see <a href="https://hl7.org/fhir/r4/parameters.html">Parameters</a>
 */
@Value(staticConstructor = "of")
public class Parameters {

  /**
   * The type of the resource.
   */
  public static final String RESOURCE_TYPE = "Parameters";

  /**
   * Represents a parameter backbone element within the Parameters resource. Only one of the
   * valueXXX fields should be set.
   */
  @Value
  @Builder(access = AccessLevel.PRIVATE)
  public static class Parameter {

    /**
     * Name from the definition.
     */
    @Nonnull
    String name;

    /**
     * Reference value for the parameter.
     */
    @Nullable
    @Builder.Default
    Reference valueReference = null;

    /**
     * String value for the parameter.
     */
    @Nullable
    @Builder.Default
    String valueString = null;

    /**
     * Instant value for the parameter.
     */
    @Nullable
    @Builder.Default
    String valueInstant = null;

    /**
     * Creates a new Parameter instance with the given name and reference value.
     *
     * @param name the name of the parameter.
     * @param valueReference the reference value.
     * @return a new Parameter instance.
     */
    @Nonnull
    public static Parameter of(@Nonnull final String name,
        final @Nonnull Reference valueReference) {
      return Parameter.builder().name(name).valueReference(valueReference).build();
    }

    /**
     * Creates a new Parameter instance with the given name and string value.
     *
     * @param name the name of the parameter.
     * @param valueString the string value.
     * @return a new Parameter instance.
     */
    @Nonnull
    public static Parameter of(@Nonnull final String name, final @Nonnull String valueString) {
      return Parameter.builder().name(name).valueString(valueString).build();
    }

    /**
     * Creates a new Parameter instance with the given name and instant value.
     *
     * @param name the name of the parameter.
     * @param valueInstant the instant value.
     * @return a new Parameter instance.
     */
    @Nonnull
    public static Parameter of(@Nonnull final String name, final @Nonnull Instant valueInstant) {
      return Parameter.builder().name(name).valueInstant(FhirUtils.formatFhirInstant(valueInstant))
          .build();
    }
  }

  /**
   * The type of the resource. Should be "Parameters".
   */
  @Nonnull
  String resourceType = RESOURCE_TYPE;

  /**
   * A collection of parameters.
   */
  @Nonnull
  List<Parameter> parameter;

  /**
   * @return the JSON representation of this Parameters instance.
   */
  @Nonnull
  public String toJson() {
    return FhirJsonSupport.toJson(this);
  }

  /**
   * @param parameters a list of parameters.
   * @return a new Parameters instance with the given parameters.
   */
  public static Parameters of(@Nonnull final Parameter... parameters) {
    return Parameters.of(List.of(parameters));
  }
}
