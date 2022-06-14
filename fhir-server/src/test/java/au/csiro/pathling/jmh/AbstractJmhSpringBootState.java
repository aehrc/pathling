package au.csiro.pathling.jmh;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.openjdk.jmh.annotations.State;


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
