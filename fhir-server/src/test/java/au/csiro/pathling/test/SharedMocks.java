package au.csiro.pathling.test;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.mockito.Mockito;

public class SharedMocks {

  private final static Map<Class<?>, Object> MOCKS = new HashMap<>();

  @Nonnull
  public static <T> T getOrCreate(@Nonnull final Class<T> clazz) {
    synchronized (MOCKS) {
      //noinspection unchecked
      return (T) MOCKS.computeIfAbsent(clazz, Mockito::mock);
    }
  }
}
