package au.csiro.pathling.jmh;

import org.springframework.test.context.TestContextManager;
import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

public class SpringBootJmhContext {

  private SpringBootJmhContext() {
  }

  private final static Map<Class<?>, TestContextManager> contextManagers = new HashMap<>();

  public static TestContextManager getOrCreate(@Nonnull final Class<?> clazz) {
    synchronized (contextManagers) {
      return contextManagers.computeIfAbsent(clazz, TestContextManager::new);
    }
  }

  public static void prepareStateInstance(@Nonnull final Object stateInstance) throws Exception {
    getOrCreate(stateInstance.getClass()).prepareTestInstance(stateInstance);
  }
}
