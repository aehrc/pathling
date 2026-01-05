package au.csiro.pathling;

import au.csiro.pathling.config.ServerConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Import;

/**
 * @author Felix Naumann
 */
@SpringBootApplication
@Import(ServerConfiguration.class)
@ServletComponentScan
@EnableAspectJAutoProxy
public class PathlingServer {

  public static void main(String[] args) {
    new SpringApplicationBuilder(PathlingServer.class).run(args);
  }
}
