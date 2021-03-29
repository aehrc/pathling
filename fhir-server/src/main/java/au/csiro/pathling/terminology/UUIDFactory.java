package au.csiro.pathling.terminology;

import java.util.UUID;

/**
 * A factory for creating UUIDs. Used mostly to allow predictable testing of UUID dependant
 * classes.
 */
@FunctionalInterface
public interface UUIDFactory {

  /**
   * Creates next UUID.
   *
   * @return a UUID.
   */
  UUID nextUUID();
}
