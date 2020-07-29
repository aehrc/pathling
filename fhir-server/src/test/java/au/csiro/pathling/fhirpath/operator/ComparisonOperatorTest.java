/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.TestElementPath;
import au.csiro.pathling.test.TestParserContext;
import au.csiro.pathling.test.assertions.Assertions;
import au.csiro.pathling.test.helpers.FhirHelpers;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class ComparisonOperatorTest {

  @Test
  void comparesDates() {
    final Optional<ElementDefinition> optionalLeftDefinition = FhirHelpers
        .getChildOfResource("MedicationRequest", "authoredOn");
    assertTrue(optionalLeftDefinition.isPresent());
    final ElementDefinition leftDefinition = optionalLeftDefinition.get();

    final Dataset<Row> leftDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("value", DataTypes.StringType)
        .withRow("Patient/abc1", "2013-06-10T15:33:22")
        .withRow("Patient/abc2", "2014-09-25T12:04:19")
        .withRow("Patient/abc3", "2018-05-18T11:03:55")
        .build();
    final ElementPath leftPath = TestElementPath
        .build("authoredOn", leftDataset, true, leftDefinition);

    final Optional<ElementDefinition> optionalRightDefinition = FhirHelpers
        .getChildOfResource("Condition", "onsetDateTime");
    assertTrue(optionalRightDefinition.isPresent());
    final ElementDefinition rightDefinition = optionalRightDefinition.get();

    final Dataset<Row> rightDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("value", DataTypes.StringType)
        .withRow("Patient/abc1", "2013-06-10T12:33:22")
        .withRow("Patient/abc2", "2014-09-25T12:04:19")
        .withRow("Patient/abc3", "2018-05-19T11:03:55")
        .build();
    final ElementPath rightPath = TestElementPath
        .build("reverseResolve(Condition.subject).onsetDateTime", rightDataset, true,
            rightDefinition);

    final ParserContext parserContext = TestParserContext.builder().build();
    final OperatorInput comparisonInput = new OperatorInput(parserContext, leftPath,
        rightPath);
    final Operator lessThan = Operator.getInstance("<=");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertTrue(result instanceof BooleanPath);
    Assertions.assertThat((ElementPath) result)
        .hasExpression("authoredOn <= reverseResolve(Condition.subject).onsetDateTime")
        .isSingular()
        .hasFhirType(FHIRDefinedType.BOOLEAN);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("value", DataTypes.BooleanType)
        .withRow("Patient/abc1", false)
        .withRow("Patient/abc2", true)
        .withRow("Patient/abc3", true)
        .build();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }
}