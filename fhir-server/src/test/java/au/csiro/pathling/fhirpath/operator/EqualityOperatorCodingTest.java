/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
public class EqualityOperatorCodingTest {

  private FhirPath left;
  private FhirPath right;
  private FhirPath literal;
  private ParserContext parserContext;

  @BeforeEach
  public void setUp() {
    parserContext = new ParserContextBuilder().build();

    final Coding coding1 = new Coding(SNOMED_URL, "56459004", null);
    coding1.setVersion("http://snomed.info/sct/32506021000036107/version/20191231");
    final Coding coding2 = new Coding(SNOMED_URL, "56459004", null);
    final Coding coding3 = new Coding(LOINC_URL, "57711-4", null);
    coding3.setVersion("2.67");
    final Coding coding4 = new Coding(LOINC_URL, "57711-4", null);
    coding4.setVersion("2.29");

    final Dataset<Row> leftDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(codingStructType())
        .withRow("abc1", rowFromCoding(coding1))
        .withRow("abc2", rowFromCoding(coding2))
        .withRow("abc3", rowFromCoding(coding3))
        .withRow("abc4", rowFromCoding(coding3))
        .withRow("abc5", null)
        .withRow("abc6", rowFromCoding(coding1))
        .withRow("abc7", null)
        .buildWithStructValue();
    left = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.CODING)
        .singular(true)
        .dataset(leftDataset)
        .idAndValueColumns()
        .build();
    final Dataset<Row> rightDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(codingStructType())
        .withRow("abc1", rowFromCoding(coding1))
        .withRow("abc2", rowFromCoding(coding1))
        .withRow("abc3", rowFromCoding(coding4))
        .withRow("abc4", rowFromCoding(coding2))
        .withRow("abc5", rowFromCoding(coding1))
        .withRow("abc6", null)
        .withRow("abc7", null)
        .buildWithStructValue();
    right = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.CODING)
        .singular(true)
        .dataset(rightDataset)
        .idAndValueColumns()
        .build();
    literal = CodingLiteralPath.fromString(
        "http://snomed.info/sct|http://snomed.info/sct/32506021000036107/version/20191231|56459004",
        left);
  }

  @Test
  public void equals() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator equalityOperator = Operator.getInstance("=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", null)
    );
  }

  @Test
  public void notEquals() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator equalityOperator = Operator.getInstance("!=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", true),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", null)
    );
  }

  @Test
  public void literalEquals() {
    final OperatorInput input = new OperatorInput(parserContext, literal, right);
    final Operator equalityOperator = Operator.getInstance("=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", true),
        RowFactory.create("abc5", true),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", null)
    );
  }

  @Test
  public void equalsLiteral() {
    final OperatorInput input = new OperatorInput(parserContext, left, literal);
    final Operator equalityOperator = Operator.getInstance("=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", true),
        RowFactory.create("abc7", null)
    );
  }

  @Test
  public void literalNotEquals() {
    final OperatorInput input = new OperatorInput(parserContext, literal, right);
    final Operator equalityOperator = Operator.getInstance("!=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", false),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", null)
    );
  }

  @Test
  public void notEqualsLiteral() {
    final OperatorInput input = new OperatorInput(parserContext, left, literal);
    final Operator equalityOperator = Operator.getInstance("!=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", true),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", false),
        RowFactory.create("abc7", null)
    );
  }

}

