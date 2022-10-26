/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.config.Configuration;
import java.io.PrintStream;
import javax.annotation.Nullable;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;

/**
 * Main class that serves as the entry point for running the Pathling server.
 *
 * @author John Grimes
 */
@SpringBootApplication
@Import(Configuration.class)
@ServletComponentScan
@EnableAspectJAutoProxy
public class PathlingServer {

  /**
   * @param args Arguments that will be passed to the main application.
   */
  public static void main(final String[] args) {
    new SpringApplicationBuilder(PathlingServer.class)
        .banner(new Banner())
        .run(args);
  }

  private static class Banner implements org.springframework.boot.Banner {

    @Override
    public void printBanner(@Nullable final Environment environment,
        @Nullable final Class<?> sourceClass, @Nullable final PrintStream out) {
      checkNotNull(out);
      out.println("    ____        __  __    ___              ");
      out.println("   / __ \\____ _/ /_/ /_  / (_)___  ____ _ ");
      out.println("  / /_/ / __ `/ __/ __ \\/ / / __ \\/ __ `/");
      out.println(" / ____/ /_/ / /_/ / / / / / / / / /_/ /   ");
      out.println("/_/    \\__,_/\\__/_/ /_/_/_/_/ /_/\\__, / ");
      out.println("                                /____/");
    }
  }

}
