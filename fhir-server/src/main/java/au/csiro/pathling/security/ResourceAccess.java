/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;


import java.lang.annotation.*;

/**
 * Identifies methods that implement access to underlying Resources. Used by {@link SecurityAspect}
 * to enforce Resource access authorization.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface ResourceAccess {

  /**
   * The type of access implemented, e.g. read or write.
   *
   * @return The type of access.
   */
  PathlingAuthority.AccessType value();
}
