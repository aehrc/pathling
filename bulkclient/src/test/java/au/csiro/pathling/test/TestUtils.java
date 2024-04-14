package au.csiro.pathling.test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import lombok.experimental.UtilityClass;
import org.apache.commons.io.IOUtils;

/**
 * Utility methods for testing.
 */
@UtilityClass
public class TestUtils {

  /**
   * Gets the contents of a resource as a string using UTF-8 encoding.
   *
   * @param resourcePath the path to the resource
   * @return the contents of the resource as a string
   */
  public static String getResourceAsString(@Nonnull final String resourcePath) {
    try {
      return IOUtils.resourceToString(resourcePath, StandardCharsets.UTF_8);
    } catch (final IOException ex) {
      throw new RuntimeException("Cannot read resource", ex);
    }
  }
}
