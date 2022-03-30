/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import ca.uhn.fhir.rest.server.exceptions.UnclassifiedServerFailureException;
import javax.annotation.Nonnull;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Aspect
@Component
@Profile("core")
public class ImportLock {

  private static boolean locked = false;

  @Around("execution(* au.csiro.pathling.update.ImportExecutor.execute(..))")
  private synchronized Object enforce(@Nonnull final ProceedingJoinPoint joinPoint)
      throws Throwable {
    if (locked) {
      throw new UnclassifiedServerFailureException(503,
          "Another import operation is currently in progress");
    }
    try {
      locked = true;
      return joinPoint.proceed();
    } finally {
      locked = false;
    }
  }

}
