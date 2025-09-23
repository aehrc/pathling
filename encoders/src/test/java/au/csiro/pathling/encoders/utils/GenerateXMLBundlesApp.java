/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific 
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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

package au.csiro.pathling.encoders.utils;

import static java.util.function.Predicate.not;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;


/**
 * Helper application, which converts JSON bundles to XML bundles.
 */
@Slf4j
public class GenerateXMLBundlesApp {

  private static final String JSON_BUNDLES_DIR = "encoders/src/test/resources/data/bundles/R4/json";
  private static final String XML_BUNDLES_DIR = "encoders/src/test/resources/data/bundles/R4/xml";


  private final FhirContext fhirContext = FhirContext.forR4();

  public static void main(final String[] args) throws Exception {
    new GenerateXMLBundlesApp().run();
  }

  private void run() throws IOException {

    final IParser jsonParser = fhirContext.newJsonParser();
    jsonParser.setOverrideResourceIdWithBundleEntryFullUrl(false);
    final IParser xmlParser = fhirContext.newXmlParser();
    xmlParser.setPrettyPrint(true);
    try (final Stream<Path> stream = Files.list(Path.of(JSON_BUNDLES_DIR))) {
      stream
          .filter(not(Files::isDirectory))
          .filter(p -> p.getFileName().toString().endsWith(".json"))
          .forEach(p -> {
            try {
              final IBaseResource resource = jsonParser.parseResource(Files.readString(p));
              final String xmlFileName = p.getFileName().toString().replace(".json", ".xml");
              Files.writeString(Path.of(XML_BUNDLES_DIR, xmlFileName),
                  xmlParser.encodeResourceToString(resource));
            } catch (final IOException e) {
              log.error("Error processing file: {}", p, e);
            }
          });
    }
  }
}

