/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.encoding;

import java.io.Serializable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hl7.fhir.r4.model.Coding;

/**
 * Implements a simplified version of {@link Coding} which is {@link Serializable} and includes an
 * equality test.
 *
 * @author John Grimes
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SimpleCoding implements Serializable {

  private static final long serialVersionUID = 6509272647875353748L;

  @Nullable
  private String system;

  @Nullable
  private String code;

  @Nullable
  private String version;

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
    final Coding coding = new Coding(system, code, null); // the last arg is display name
    coding.setVersion(version);
    return coding;
  }

  /**
   * Checks if the coding has both the system and code defined, so that it represent a known
   * deterministic code.
   *
   * @return if the coding is defined.
   */
  public boolean isDefined() {
    return system != null && code != null;
  }

  /**
   * Checks if the coding is versioned.
   *
   * @return true if the coding is versioned
   */
  public boolean isVersioned() {
    return version != null;
  }

  /**
   * Strips the version information from the coding
   *
   * @return unversioned coding
   */
  @Nonnull
  public SimpleCoding toNonVersioned() {
    return isVersioned()
           ?
           new SimpleCoding(this.system, this.code)
           : this;
  }

}