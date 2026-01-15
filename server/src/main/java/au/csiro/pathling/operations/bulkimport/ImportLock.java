/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.bulkimport;

import ca.uhn.fhir.rest.server.exceptions.UnclassifiedServerFailureException;
import jakarta.annotation.Nonnull;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * An aspect that enforces mutual exclusion on import operations.
 *
 * @author John Grimes
 */
@Aspect
@Component
public class ImportLock {

  private static boolean locked = false;

  @Around("execution(* au.csiro.pathling.operations.bulkimport.ImportExecutor.execute(..))")
  private synchronized Object enforce(@Nonnull final ProceedingJoinPoint joinPoint)
      throws Throwable {
    if (locked) {
      throw new UnclassifiedServerFailureException(
          503, "Another import operation is currently in progress");
    }
    try {
      locked = true;
      return joinPoint.proceed();
    } finally {
      locked = false;
    }
  }
}
