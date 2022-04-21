/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.helpers.SparkHelpers.codingStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCoding;
import static au.csiro.pathling.test.helpers.TestHelpers.LOINC_URL;
import static au.csiro.pathling.test.helpers.TestHelpers.SNOMED_URL;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
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
class EqualityOperatorCodingTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  static final String ID_ALIAS = "_abc123";

  FhirPath left;
  FhirPath right;
  FhirPath literalSnomedAll;
  FhirPath literalLoincSystemCode;
  ParserContext parserContext;

  @BeforeEach
  void setUp() {
    // all components
    final Coding coding1 = new Coding(SNOMED_URL, "56459004", null);
    coding1.setVersion("http://snomed.info/sct/32506021000036107/version/20191231");
    coding1.setDisplay("Display name");
    coding1.setUserSelected(true);
    coding1.setId("some-fake-id");

    final Coding coding2 = new Coding(SNOMED_URL, "56459004", null);
    coding2.setVersion("http://snomed.info/sct/32506021000036107/version/20191231");
    coding2.setDisplay("Display name");

    final Coding coding3 = new Coding(SNOMED_URL, "56459004", null);
    coding3.setVersion("http://snomed.info/sct/32506021000036107/version/20191231");

    final Coding coding4 = new Coding(LOINC_URL, "222|33", null);
    coding4.setId("fake-id-1");
    final Coding coding5 = new Coding(LOINC_URL, "222|33", null);
    coding5.setId("fake-id-2");

    final Coding coding6 = new Coding(LOINC_URL, "56459004", null);
    coding6.setVersion("http://snomed.info/sct/32506021000036107/version/20191231");
    coding6.setDisplay("Display name");
    coding6.setUserSelected(true);
    coding6.setId("some-fake-id");

    final Coding coding1_other = coding1.copy();
    coding1_other.setId("some-other-fake-id");

    final Dataset<Row> leftDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withStructTypeColumns(codingStructType())
        .withRow("patient-1", rowFromCoding(coding1))
        .withRow("patient-2", rowFromCoding(coding2))
        .withRow("patient-3", rowFromCoding(coding3))
        .withRow("patient-4", rowFromCoding(coding4))
        .withRow("patient-5", rowFromCoding(coding5))
        .withRow("patient-6", rowFromCoding(coding6))
        .withRow("patient-7", null)
        .build();
    left = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .singular(true)
        .dataset(leftDataset)
        .idAndValueColumns()
        .build();
    final Dataset<Row> rightDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withStructTypeColumns(codingStructType())
        .withRow("patient-1", rowFromCoding(coding1_other))
        .withRow("patient-2", rowFromCoding(coding3))
        .withRow("patient-3", rowFromCoding(coding3))
        .withRow("patient-4", rowFromCoding(coding5))
        .withRow("patient-5", rowFromCoding(coding6))
        .withRow("patient-6", null)
        .withRow("patient-7", null)
        .build();
    right = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .singular(true)
        .dataset(rightDataset)
        .idAndValueColumns()
        .build();
    literalSnomedAll = CodingLiteralPath.fromString(
        "http://snomed.info/sct|56459004|http://snomed.info/sct/32506021000036107/version/20191231|'Display name'|true",
        left);
    literalLoincSystemCode = CodingLiteralPath.fromString("http://loinc.org|'222|33'", left);

    parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(left.getIdColumn()))
        .build();
  }

  @Test
  void equals() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator equalityOperator = Operator.getInstance("=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", false),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", true),
        RowFactory.create("patient-5", false),
        RowFactory.create("patient-6", null),
        RowFactory.create("patient-7", null)
    );
  }

  @Test
  void notEquals() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator equalityOperator = Operator.getInstance("!=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", true),
        RowFactory.create("patient-3", false),
        RowFactory.create("patient-4", false),
        RowFactory.create("patient-5", true),
        RowFactory.create("patient-6", null),
        RowFactory.create("patient-7", null)
    );
  }

  @Test
  void literalEquals() {
    final OperatorInput input = new OperatorInput(parserContext, literalLoincSystemCode, left);
    final Operator equalityOperator = Operator.getInstance("=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", false),
        RowFactory.create("patient-3", false),
        RowFactory.create("patient-4", true),
        RowFactory.create("patient-5", true),
        RowFactory.create("patient-6", false),
        RowFactory.create("patient-7", null)
    );
  }

  @Test
  void equalsLiteral() {
    final OperatorInput input = new OperatorInput(parserContext, left, literalSnomedAll);
    final Operator equalityOperator = Operator.getInstance("=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", false),
        RowFactory.create("patient-3", false),
        RowFactory.create("patient-4", false),
        RowFactory.create("patient-5", false),
        RowFactory.create("patient-6", false),
        RowFactory.create("patient-7", null)
    );
  }

  @Test
  void literalNotEquals() {
    final OperatorInput input = new OperatorInput(parserContext, literalLoincSystemCode, left);
    final Operator equalityOperator = Operator.getInstance("!=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", true),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", false),
        RowFactory.create("patient-5", false),
        RowFactory.create("patient-6", true),
        RowFactory.create("patient-7", null)
    );
  }

  @Test
  void notEqualsLiteral() {
    final OperatorInput input = new OperatorInput(parserContext, left, literalSnomedAll);
    final Operator equalityOperator = Operator.getInstance("!=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", true),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", true),
        RowFactory.create("patient-5", true),
        RowFactory.create("patient-6", true),
        RowFactory.create("patient-7", null)
    );
  }

}

