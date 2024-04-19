/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.utilities;

import jakarta.annotation.Nonnull;

/**
 * @author John Grimes
 */
public abstract class Versioning {

  /**
   * @param semanticVersion A version number conforming to Semantic Versioning 2.0.0
   * @return The major version component of the version number
   * @see <a href="https://semver.org/spec/v2.0.0.html">Semantic Versioning 2.0.0</a>
   */
  @Nonnull
  public static String getMajorVersion(@Nonnull final String semanticVersion) {
    return semanticVersion.split("\\.")[0];
  }

}
