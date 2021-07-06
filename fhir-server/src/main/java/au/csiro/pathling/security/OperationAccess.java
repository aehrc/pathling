/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;


import java.lang.annotation.*;

/**
 * Identifies methods that require certain granted authority to be executed.
 * <p>
 * Used by {@link SecurityAspect} to enforce resource access authorization.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface OperationAccess {

  /**
   * The name of the operation being accessed.
   *
   * @return The name of the the operation.
   */
  String value();
}
