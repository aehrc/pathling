package au.csiro.pathling.terminology;

import java.util.UUID;
import javax.annotation.Nonnull;

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
  @Nonnull
  UUID nextUUID();
}
