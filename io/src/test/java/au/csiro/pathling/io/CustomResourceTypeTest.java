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

package au.csiro.pathling.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

/**
 * Tests that the routes that parse with HAPI read and write a resource type that is not part of
 * FHIR, where the definitions describe one, as they do a standard type.
 */
class CustomResourceTypeTest {

  @Nonnull
  private static final FhirReader READER = FhirReader.of(TransformFixtures.spark(), definitions());

  @Nonnull private static final FhirWriter WRITER = FhirWriter.of(definitions());

  @Nonnull
  private static final String WIDGET_XML =
      "<Widget xmlns=\"http://hl7.org/fhir\"><id value=\"w1\"/><label value=\"bolt\"/></Widget>";

  @Nonnull
  private static final String WIDGET_JSON =
      "{\"resourceType\":\"Widget\",\"id\":\"w1\",\"label\":\"bolt\"}";

  @Test
  void readsACustomTypeFromXml() {
    assertEquals(List.of("w1"), ids(READER.xml().read("Widget", documents(WIDGET_XML))));
  }

  @Test
  void readsACustomTypeFromAnXmlBundle() {
    final String bundle =
        "<Bundle xmlns=\"http://hl7.org/fhir\"><type value=\"collection\"/><entry><resource>"
            + WIDGET_XML.replace(" xmlns=\"http://hl7.org/fhir\"", "")
            + "</resource></entry></Bundle>";

    assertEquals(List.of("w1"), ids(READER.xml().readBundles("Widget", documents(bundle))));
  }

  @Test
  void readsAStandardTypeFromABundleThatAlsoCarriesACustomOne() {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"resource\":"
            + WIDGET_JSON
            + "},{\"resource\":{\"resourceType\":\"Patient\",\"id\":\"p1\"}}]}";

    assertEquals(List.of("p1"), ids(READER.json().readBundles("Patient", documents(bundle))));
    assertEquals(List.of("w1"), ids(READER.json().readBundles("Widget", documents(bundle))));
  }

  @Test
  void writesACustomTypeAsXml() {
    final Dataset<Row> stored = READER.json().read("Widget", documents(WIDGET_JSON));

    final List<String> written = WRITER.xml().write("Widget", stored).collectAsList();

    assertEquals(1, written.size());
    assertEquals(true, written.get(0).contains("<label value=\"bolt\"/>"));
  }

  @Nonnull
  private static FhirDefinitionContext definitions() {
    final FhirContext context = FhirContext.forR4();
    context.registerCustomType(WidgetResource.class);
    return FhirDefinitionContext.of(context);
  }

  @Nonnull
  private static Dataset<String> documents(@Nonnull final String... documents) {
    return TransformFixtures.spark().createDataset(List.of(documents), Encoders.STRING());
  }

  @Nonnull
  private static List<String> ids(@Nonnull final Dataset<Row> resources) {
    return resources.select("id").as(Encoders.STRING()).collectAsList();
  }
}
