package au.csiro.pathling.test;

import static au.csiro.pathling.utilities.Preconditions.check;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

@Slf4j
public abstract class TestResources {

  @Nonnull
  private static ClassLoader getClassLoader() {
    final ClassLoader object = Thread.currentThread().getContextClassLoader();
    return requireNonNull(object);
  }

  @Nonnull
  public static URL getResourceAsUrl(@Nonnull final String name) {
    final ClassLoader loader = getClassLoader();
    final URL object = loader.getResource(name);
    return requireNonNull(object);
  }

  @Nonnull
  public static InputStream getResourceAsStream(@Nonnull final String name) {
    final ClassLoader loader = getClassLoader();
    final InputStream inputStream = loader.getResourceAsStream(name);
    check(Objects.nonNull(inputStream), "Failed to load resource from : '%s'", name);
    return requireNonNull(inputStream);
  }

  @Nonnull
  public static String getResourceAsString(@Nonnull final String name) {
    try {
      final InputStream expectedStream = getResourceAsStream(name);
      final StringWriter writer = new StringWriter();
      IOUtils.copy(expectedStream, writer, UTF_8);
      return writer.toString();
    } catch (final IOException e) {
      throw new RuntimeException("Problem retrieving test resource", e);
    }
  }
}
