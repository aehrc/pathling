/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.jmh;

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import org.springframework.test.annotation.DirtiesContext.HierarchyMode;
import org.springframework.test.context.TestContextManager;

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


  /**
   * Cleans up the contexts of all the classes that have been autowired using this class.
   * This also destroys and clean up all the test application contexts created thus far.
   */
  public static void cleanUpAll() {
    synchronized (contextManagers) {
      contextManagers.forEach((k, v) -> v.getTestContext().markApplicationContextDirty(
          HierarchyMode.EXHAUSTIVE));
      contextManagers.clear();
    }
  }
}
