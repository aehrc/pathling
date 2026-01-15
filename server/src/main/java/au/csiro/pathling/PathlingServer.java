/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.config.ServerConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Import;

/**
 * The main entry point for the Pathling FHIR server application.
 *
 * @author Felix Naumann
 */
@SpringBootApplication
@Import(ServerConfiguration.class)
@ServletComponentScan
@EnableAspectJAutoProxy
public class PathlingServer {

  /**
   * Main entry point for the Pathling server application.
   *
   * @param args command line arguments
   */
  public static void main(final String[] args) {
    new SpringApplicationBuilder(PathlingServer.class).run(args);
  }
}
