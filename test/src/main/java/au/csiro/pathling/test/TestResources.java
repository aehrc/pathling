package au.csiro.pathling.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.IOUtils;

public abstract class TestResources {

  private static ClassLoader getClassLoader() {
    final ClassLoader object = Thread.currentThread().getContextClassLoader();
    return requireNonNull(object);
  }

  public static URL getResourceAsUrl(final String name) {
    final ClassLoader loader = getClassLoader();
    final URL object = loader.getResource(name);
    return requireNonNull(object, "Test resource not found: " + name);
  }

  private static InputStream getResourceAsStream(final String name) {
    final ClassLoader loader = getClassLoader();
    final InputStream inputStream = loader.getResourceAsStream(name);
    requireNonNull(inputStream, "Test resource not found: " + name);
    return inputStream;
  }

  public static String getResourceAsString(final String name) {
    try (final InputStream expectedStream = getResourceAsStream(name)) {
      final StringWriter writer = new StringWriter();
      IOUtils.copy(expectedStream, writer, UTF_8);
      return writer.toString();
    } catch (final IOException e) {
      throw new RuntimeException("Problem retrieving test resource", e);
    }
  }

  public static void deleteRecursively(final Path directory) throws IOException {
    if (Files.isDirectory(directory)) {
      try (final DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
        for (final Path entry : stream) {
          deleteRecursively(entry);
        }
      }
    }
    Files.delete(directory);
  }

}