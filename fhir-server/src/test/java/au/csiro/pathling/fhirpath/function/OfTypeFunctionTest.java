/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.helpers.SparkHelpers.referenceStructType;
import static au.csiro.pathling.utilities.Preconditions.check;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.ParserContextBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class OfTypeFunctionTest {

  private FhirContext fhirContext;
  private ResourceReader mockReader;

  @BeforeEach
  void setUp() {
    fhirContext = FhirHelpers.getFhirContext();
    mockReader = mock(ResourceReader.class);
  }

  @Test
  void resolvesPolymorphicReference() {
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withTypeColumn()
        .withStructTypeColumns(referenceStructType())
        .withRow("Encounter/xyz1", "Patient", RowFactory.create(null, "Patient/abc1", null))
        .withRow("Encounter/xyz2", "Patient", RowFactory.create(null, "Patient/abc3", null))
        .withRow("Encounter/xyz3", "Patient", RowFactory.create(null, "Patient/abc2", null))
        .withRow("Encounter/xyz4", "Patient", RowFactory.create(null, "Patient/abc2", null))
        .withRow("Encounter/xyz5", "Group", RowFactory.create(null, "Group/def1", null))
        .buildWithStructValue();
    final Column idColumn = inputDataset.col("id");
    final Column typeColumn = inputDataset.col("type");
    final Column valueColumn = inputDataset.col("value");
    final UntypedResourcePath inputPath = new UntypedResourcePath(
        "Encounter.subject.resolve()", inputDataset, Optional.of(idColumn), valueColumn,
        true, typeColumn, new HashSet<>(Arrays.asList(ResourceType.PATIENT, ResourceType.GROUP)));

    final Dataset<Row> argumentDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("Patient/abc1", "female", true)
        .withRow("Patient/abc2", "female", false)
        .withRow("Patient/abc3", "male", true)
        .build();
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(argumentDataset);
    final ResourcePath argumentPath = ResourcePath
        .build(fhirContext, mockReader, ResourceType.PATIENT, "Patient", false);
    check(inputPath.getIdColumn().isPresent());

    final ParserContext parserContext = new ParserContextBuilder()
        .idColumn(inputPath.getIdColumn().get())
        .build();
    final NamedFunctionInput ofTypeInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentPath));
    final NamedFunction count = NamedFunction.getInstance("ofType");
    final FhirPath result = count.invoke(ofTypeInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("Encounter.subject.resolve().ofType(Patient)")
        .isSingular()
        .hasResourceType(ResourceType.PATIENT);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("gender", DataTypes.StringType)
        .withStructColumn("active", DataTypes.BooleanType)
        .withRow("Encounter/xyz1", RowFactory.create("Patient/abc1", "female", true))
        .withRow("Encounter/xyz2", RowFactory.create("Patient/abc3", "male", true))
        .withRow("Encounter/xyz3", RowFactory.create("Patient/abc2", "female", false))
        .withRow("Encounter/xyz4", RowFactory.create("Patient/abc2", "female", false))
        .withRow("Encounter/xyz5", null)
        .buildWithStructValue();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

}