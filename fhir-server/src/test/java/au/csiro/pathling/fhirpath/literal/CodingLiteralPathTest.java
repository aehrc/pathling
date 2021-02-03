/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.io.ResourceReader;
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
  private SparkSession spark;

  private ResourcePath inputContext;

  @BeforeEach
  void setUp() {
    final ResourceReader resourceReader = mock(ResourceReader.class);
    final Dataset<Row> inputContextDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withRow("observation-1")
        .withRow("observation-2")
        .withRow("observation-3")
        .withRow("observation-4")
        .withRow("observation-5")
        .build();
    when(resourceReader.read(ResourceType.OBSERVATION)).thenReturn(inputContextDataset);
    inputContext = new ResourcePathBuilder(spark)
        .expression("Observation")
        .resourceType(ResourceType.OBSERVATION)
        .resourceReader(resourceReader)
        .singular(true)
        .build();
  }

  @Test
  void roundTrip() {
    final String expression = "http://snomed.info/sct|http://snomed.info/sct/32506021000036107/version/20201231|166056000";
    final CodingLiteralPath codingLiteralPath = CodingLiteralPath.fromString(
        expression,
        inputContext);
    final Coding literalValue = codingLiteralPath.getLiteralValue();
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
    final Coding literalValue = codingLiteralPath.getLiteralValue();
    assertEquals("http://snomed.info/sct", literalValue.getSystem());
    assertNull(literalValue.getVersion());
    assertEquals("166056000", literalValue.getCode());

    final String actualExpression = codingLiteralPath.getExpression();
    assertEquals(expression, actualExpression);
  }

  @Test
  void fromStringWithQuotedComponent() {
    final String expression =
        "http://snomed.info/sct|http://snomed.info/sct/32506021000036107/version/20201231|"
            + "'397956004 |Prosthetic arthroplasty of the hip|: 363704007 |Procedure site| = "
            + "( 24136001 |Hip joint structure|: 272741003 |Laterality| =  7771000 |Left| )'";
    final CodingLiteralPath codingLiteralPath = CodingLiteralPath
        .fromString(expression, inputContext);
    final Coding literalValue = codingLiteralPath.getLiteralValue();
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
  void fromStringWithQuotedComponentWithComma() {
    final String expression =
        "http://snomed.info/sct|http://snomed.info/sct/32506021000036107/version/20201231|'46,2'";
    final CodingLiteralPath codingLiteralPath = CodingLiteralPath
        .fromString(expression, inputContext);
    final Coding literalValue = codingLiteralPath.getLiteralValue();
    assertEquals("http://snomed.info/sct", literalValue.getSystem());
    assertEquals("http://snomed.info/sct/32506021000036107/version/20201231",
        literalValue.getVersion());
    assertEquals("46,2", literalValue.getCode());

    final String actualExpression = codingLiteralPath.getExpression();
    assertEquals(expression, actualExpression);
  }

  @Test
  void fromMalformedString() {
    assertThrows(IllegalArgumentException.class, () -> CodingLiteralPath.fromString(
        "http://snomed.info/sct", inputContext));
  }

}