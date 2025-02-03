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

package au.csiro.pathling.fhirpath.parser;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;

public class QuantityParserTest extends AbstractParserTest {

  @Test
  void lengthObservationComparison() {
    assertThatResultOf(ResourceType.OBSERVATION, "valueQuantity < 1.5 'm'")
        .isElementPath(BooleanCollection.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/lengthObservationComparison.tsv");
  }

  @Test
  void lengthObservationSubtraction() {
    assertThatResultOf(ResourceType.OBSERVATION, "valueQuantity > (valueQuantity - 2 'g/dL')")
        .isElementPath(BooleanCollection.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/lengthObservationSubtraction.tsv");
  }

}
