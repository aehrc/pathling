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

package au.csiro.pathling.fhirpath.encoding;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Coding;

/**
 * Immutable comparable and printable version of fhir Coding
 */

@AllArgsConstructor(staticName = "of")
@ToString
@Getter
public class ImmutableCoding implements Serializable {

  private static final long serialVersionUID = -2461921559175440312L;

  @Nullable
  private final String system;
  @Nullable
  private final String version;
  @Nullable
  private final String code;
  @Nullable
  private final String display;
  @Nullable
  protected final Boolean userSelected;

  /**
   * Conversion to fhir Coding.
   *
   * @return the corresponding fhir Coding.
   */
  @Nonnull
  public Coding toCoding() {
    return new Coding(system, code, display).setVersion(version).setUserSelectedElement(
        userSelected != null
        ? new BooleanType(userSelected)
        : null);
  }

  /**
   * Conversion from a fhir Coding.
   *
   * @param coding the fhir Coding to convert.
   * @return the corresponding ImmutableCoding.
   */
  @Nonnull
  public static ImmutableCoding of(@Nonnull final Coding coding) {
    return ImmutableCoding.of(coding.getSystem(), coding.getVersion(), coding.getCode(),
        coding.getDisplay(), coding.hasUserSelected()
                             ? coding.getUserSelected()
                             : null);
  }

  /**
   * Static constructor from basic elements.
   *
   * @param system the code system.
   * @param code the code.
   * @param display the display name.
   * @return the immutable coding with desired properties.
   */
  @Nonnull
  public static ImmutableCoding of(@Nonnull final String system, @Nonnull final String code,
      @Nonnull final String display) {
    return ImmutableCoding.of(system, null, code, display, null);
  }

  /**
   * Constructs an empty coding (all values null).
   *
   * @return empty coding.
   */
  @Nonnull
  public static ImmutableCoding empty() {
    return ImmutableCoding.of(null, null, null, null, null);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ImmutableCoding that = (ImmutableCoding) o;
    return Objects.equals(system, that.system) && Objects.equals(version,
        that.version) && Objects.equals(code, that.code);
  }

  @Override
  public int hashCode() {
    return Objects.hash(system, version, code);
  }

}
