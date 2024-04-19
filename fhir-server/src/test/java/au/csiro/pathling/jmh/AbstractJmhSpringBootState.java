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

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;


/**
 * Base class for implementing JMH state objects that can be autowired with SpringBoot test
 * contexts. The subclasses need to be annotated with a desired {@link State} annotation and can
 * also have spring annotations relevant to the test context configuration (e.g.:{@link
 * ActiveProfiles}).
 * <p>
 * Note: JMH orders the calls to setup methods (in case there is more than one in a benchmark)
 * sorting them by full qualified methods names that include package, class and the method name.
 * Since we want the spring autowiring to happen first we need to insure that the name of {@link
 * #wireUp()} comes up first which is now achieved by placing this class in 'au.csiro.pathling.jmh'
 * package and all the classes that use it in 'au.csiro.pathling.test.benchmark'.
 */
@SpringBootTest
public abstract class AbstractJmhSpringBootState {

  @Setup(Level.Trial)
  public void wireUp() throws Exception {
    SpringBootJmhContext.autowireWithTestContext(this);
  }
}
