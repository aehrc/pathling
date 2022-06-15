package au.csiro.pathling.jmh;

import org.springframework.test.context.TestContextManager;
import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * This class hijacks the mechanism that SpringBoot uses for auto wiring of SpringBootTests.
 * Instance of any class annotated with @SpringBootTest (and other relevant annotations related to
 * spring context configuration) can be autowired  by the {@link #autowireWithTestContext(Object)}
 */
public class SpringBootJmhContext {

  private SpringBootJmhContext() {
  }

  private final static Map<Class<?>, TestContextManager> contextManagers = new HashMap<>();


  private static TestContextManager getOrCreate(@Nonnull final Class<?> clazz) {
    synchronized (contextManagers) {
      return contextManagers.computeIfAbsent(clazz, TestContextManager::new);
    }
  }

  /**
   * Autowires an instance of a class annotated with @SpringBootTest using the test context
   * configured as if it was a spring boot unit tests. Although the class needs to be annotated as
   * SpringBootTest it does not need to be an actual unit test.
   *
   * @param testLikeObject the instance of the class to autowire.
   */
  public static void autowireWithTestContext(@Nonnull final Object testLikeObject)
      throws Exception {
    getOrCreate(testLikeObject.getClass()).prepareTestInstance(testLikeObject);
  }
}
