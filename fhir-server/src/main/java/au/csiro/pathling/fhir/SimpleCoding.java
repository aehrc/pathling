/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Data;
import org.hl7.fhir.r4.model.Coding;

/**
 * Implements a simplified version of {@link Coding} which is {@link Serializable} and includes an
 * equality test.
 *
 * @author John Grimes
 */
@Data
public class SimpleCoding implements Serializable {

  private static final long serialVersionUID = 6509272647875353748L;

  @Nullable
  private final String system;

  @Nullable
  private final String code;

  @Nullable
  private final String version;

  /**
   * @param system The code system URI for this coding
   * @param code The code itself
   * @param version The version of the code system, if any
   */
  public SimpleCoding(@Nullable final String system, @Nullable final String code,
      @Nullable final String version) {
    this.system = system;
    this.code = code;
    this.version = version;
  }

  /**
   * @param system The code system URI for this coding
   * @param code The code itself
   */
  public SimpleCoding(@Nullable final String system, @Nullable final String code) {
    this(system, code, null);
  }

  /**
   * @param coding A {@link Coding} object to copy values from
   */
  public SimpleCoding(@Nonnull final Coding coding) {
    this.system = coding.getSystem();
    this.code = coding.getCode();
    this.version = coding.getVersion();
  }

  /**
   * @return A new {@link Coding} containing the values from this SimpleCoding
   */
  @Nonnull
  public Coding toCoding() {
    return new Coding(system, code, version);
  }

  public boolean isNull() {
    return system == null || code == null;
  }

  public boolean isNotNull() {
    return !isNull();
  }

  public boolean isVersioned() {
    return version != null;
  }

  /**
   * @return A new SimpleCoding without any version information
   */
  @Nonnull
  public SimpleCoding toNonVersioned() {
    return new SimpleCoding(this.system, this.code);
  }

  /**
   * Performs a comparison of two SimpleCoding objects using the greater specificity of the two
   * operands. If one of the codings has a version, version will be considered.
   */
  @Override
  public boolean equals(@Nullable final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final SimpleCoding other = (SimpleCoding) obj;
    if (isNull() || other.isNull()) {
      return false;
    }
    if (isVersioned() || other.isVersioned()) {
      //noinspection ConstantConditions
      return system.equals(other.system) && code.equals(other.code) && version
          .equals(other.version);
    } else {
      //noinspection ConstantConditions
      return system.equals(other.system) && code.equals(other.code);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(system, code, version);
  }

  @Override
  public String toString() {
    return "SimpleCoding{" +
        "system='" + system + '\'' +
        ", code='" + code + '\'' +
        ", version='" + version + '\'' +
        '}';
  }

}