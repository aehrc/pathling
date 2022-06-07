package au.csiro.pathling.jmh;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestContextManager;

@SpringBootTest
public abstract class AbstractJmhSpringBootState {

  @Setup(Level.Trial)
  public void wireUp() throws Exception {
    SpringBootJmhContext.prepareStateInstance(this);
  }
}
