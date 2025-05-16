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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class DateTimeArithmeticParserTest extends AbstractParserTest {

  @Test
  void lengthOfEncounter() {
    assertThatResultOf(ResourceType.ENCOUNTER,
        "(period.start + 20 minutes) > period.end")
        .isElementPath(BooleanCollection.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/lengthOfEncounter.tsv");
  }

  @Test
  void ageAtTimeOfEncounter() {
    assertThatResultOf(ResourceType.ENCOUNTER,
        "period.start > (subject.resolve().ofType(Patient).birthDate + 60 years)")
        .isElementPath(BooleanCollection.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/ageAtTimeOfEncounter.tsv");
  }

}
