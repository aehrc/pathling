/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import java.util.UUID;
import javax.annotation.Nonnull;

/**
 * A factory for creating UUIDs. Used mostly to allow predictable testing of UUID dependent
 * classes.
 */
@FunctionalInterface
public interface UUIDFactory {

  /**
   * Creates next UUID.
   *
   * @return a UUID.
   */
  @Nonnull
  UUID nextUUID();
}
