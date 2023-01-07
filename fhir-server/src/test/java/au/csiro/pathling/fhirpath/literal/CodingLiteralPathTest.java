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

package au.csiro.pathling.fhirpath.literal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
class CodingLiteralPathTest {

  @Autowired
  SparkSession spark;

  ResourcePath inputContext;

  @BeforeEach
  void setUp() {
    final Database database = mock(Database.class);
    final Dataset<Row> inputContextDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withRow("observation-1")
        .withRow("observation-2")
        .withRow("observation-3")
        .withRow("observation-4")
        .withRow("observation-5")
        .build();
    when(database.read(ResourceType.OBSERVATION)).thenReturn(inputContextDataset);
    inputContext = new ResourcePathBuilder(spark)
        .expression("Observation")
        .resourceType(ResourceType.OBSERVATION)
        .database(database)
        .singular(true)
        .build();
  }

  @Test
  void roundTrip() {
    final String expression = "http://snomed.info/sct|166056000|http://snomed.info/sct/32506021000036107/version/20201231";
    final CodingLiteralPath codingLiteralPath = CodingLiteralPath.fromString(
        expression,
        inputContext);
    final Coding literalValue = codingLiteralPath.getValue();
    assertEquals("http://snomed.info/sct", literalValue.getSystem());
    assertEquals("http://snomed.info/sct/32506021000036107/version/20201231",
        literalValue.getVersion());
    assertEquals("166056000", literalValue.getCode());

    final String actualExpression = codingLiteralPath.getExpression();
    assertEquals(expression, actualExpression);
  }

  @Test
  void roundTripNoVersion() {
    final String expression = "http://snomed.info/sct|166056000";
    final CodingLiteralPath codingLiteralPath = CodingLiteralPath
        .fromString(expression, inputContext);
    final Coding literalValue = codingLiteralPath.getValue();
    assertEquals("http://snomed.info/sct", literalValue.getSystem());
    assertNull(literalValue.getVersion());
    assertEquals("166056000", literalValue.getCode());

    final String actualExpression = codingLiteralPath.getExpression();
    assertEquals(expression, actualExpression);
  }

  @Test
  void roundTripWithQuotedComponent() {
    final String expression =
        "http://snomed.info/sct"
            + "|'397956004 |Prosthetic arthroplasty of the hip|: 363704007 |Procedure site| = "
            + "( 24136001 |Hip joint structure|: 272741003 |Laterality| =  7771000 |Left| )'"
            + "|http://snomed.info/sct/32506021000036107/version/20201231";
    final CodingLiteralPath codingLiteralPath = CodingLiteralPath
        .fromString(expression, inputContext);
    final Coding literalValue = codingLiteralPath.getValue();
    assertEquals("http://snomed.info/sct", literalValue.getSystem());
    assertEquals("http://snomed.info/sct/32506021000036107/version/20201231",
        literalValue.getVersion());
    assertEquals(
        "397956004 |Prosthetic arthroplasty of the hip|: 363704007 |Procedure site| = "
            + "( 24136001 |Hip joint structure|: 272741003 |Laterality| =  7771000 |Left| )",
        literalValue.getCode());

    final String actualExpression = codingLiteralPath.getExpression();
    assertEquals(expression, actualExpression);
  }

  @Test
  void roundTripWithQuotedComponentWithComma() {
    final String expression =
        "http://snomed.info/sct|'46,2'|http://snomed.info/sct/32506021000036107/version/20201231";
    final CodingLiteralPath codingLiteralPath = CodingLiteralPath
        .fromString(expression, inputContext);
    final Coding literalValue = codingLiteralPath.getValue();
    assertEquals("http://snomed.info/sct", literalValue.getSystem());
    assertEquals("http://snomed.info/sct/32506021000036107/version/20201231",
        literalValue.getVersion());
    assertEquals("46,2", literalValue.getCode());

    final String actualExpression = codingLiteralPath.getExpression();
    assertEquals(expression, actualExpression);
  }

  @Test
  void roundTripWithQuotedComponentWithSingleQuote() {
    final String expression = "'Someone\\'s CodeSystem'|166056000";
    final CodingLiteralPath codingLiteralPath = CodingLiteralPath
        .fromString(expression, inputContext);
    final Coding literalValue = codingLiteralPath.getValue();
    assertEquals("Someone's CodeSystem", literalValue.getSystem());
    assertEquals("166056000", literalValue.getCode());

    final String actualExpression = codingLiteralPath.getExpression();
    assertEquals(expression, actualExpression);
  }

  @Test
  void roundTripWithQuotedComponentWithSpace() {
    final String expression = "'Some CodeSystem'|166056000";
    final CodingLiteralPath codingLiteralPath = CodingLiteralPath
        .fromString(expression, inputContext);
    final Coding literalValue = codingLiteralPath.getValue();
    assertEquals("Some CodeSystem", literalValue.getSystem());
    assertEquals("166056000", literalValue.getCode());

    final String actualExpression = codingLiteralPath.getExpression();
    assertEquals(expression, actualExpression);
  }

  @Test
  void fromMalformedString() {
    assertThrows(IllegalArgumentException.class, () -> CodingLiteralPath.fromString(
        "http://snomed.info/sct", inputContext));
  }

}
