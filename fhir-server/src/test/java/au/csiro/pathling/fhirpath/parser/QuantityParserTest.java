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

package au.csiro.pathling.fhirpath.parser;

import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import java.util.Collections;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;

public class QuantityParserTest extends AbstractParserTest {

  @Test
  void lengthObservationComparison() {
    final ResourcePath subjectResource = ResourcePath
        .build(fhirContext, database, ResourceType.OBSERVATION, ResourceType.OBSERVATION.toCode(),
            true);
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(terminologyServiceFactory)
        .database(database)
        .inputContext(subjectResource)
        .groupingColumns(Collections.singletonList(subjectResource.getIdColumn()))
        .build();
    parser = new Parser(parserContext);

    assertThatResultOf("valueQuantity < 1.5 'm'")
        .isElementPath(BooleanPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/lengthObservationComparison.csv");
  }

  @Test
  void lengthObservationSubtraction() {
    final ResourcePath subjectResource = ResourcePath
        .build(fhirContext, database, ResourceType.OBSERVATION, ResourceType.OBSERVATION.toCode(),
            true);
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(terminologyServiceFactory)
        .database(database)
        .inputContext(subjectResource)
        .groupingColumns(Collections.singletonList(subjectResource.getIdColumn()))
        .build();
    parser = new Parser(parserContext);

    assertThatResultOf("valueQuantity > (valueQuantity - 2 'g/dL')")
        .isElementPath(BooleanPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/lengthObservationSubtraction.csv");
  }

}
