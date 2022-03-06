/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.helpers.SparkHelpers.getIdAndValueColumns;
import static au.csiro.pathling.test.helpers.SparkHelpers.referenceStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.SparkHelpers.IdAndValueColumns;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
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
class ReverseResolveFunctionTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  Database database;

  @BeforeEach
  void setUp() {
    database = mock(Database.class);
  }

  @Test
  void reverseResolve() {
    final Dataset<Row> patientDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("patient-1", "female", true)
        .withRow("patient-2", "female", false)
        .withRow("patient-3", "male", true)
        .withRow("patient-4", "male", true)
        .build();
    when(database.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);
    final ResourcePath inputPath = ResourcePath
        .build(fhirContext, database, ResourceType.PATIENT, "Patient", true);

    final DatasetBuilder encounterDatasetBuilder = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("status", DataTypes.StringType)
        .withRow("encounter-1", "planned")
        .withRow("encounter-2", "arrived")
        .withRow("encounter-3", "triaged")
        .withRow("encounter-4", "in-progress")
        .withRow("encounter-5", "onleave");
    final Dataset<Row> encounterDataset = encounterDatasetBuilder.build();
    when(database.read(ResourceType.ENCOUNTER)).thenReturn(encounterDataset);
    final ResourcePath originPath = ResourcePath
        .build(fhirContext, database, ResourceType.ENCOUNTER, "Encounter", false);

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "Encounter", "subject");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> argumentDatasetPreJoin = new DatasetBuilder(spark)
        .withIdColumn()
        .withStructTypeColumns(referenceStructType())
        .withRow("encounter-1", RowFactory.create(null, "Patient/patient-1", null))
        .withRow("encounter-2", RowFactory.create(null, "Patient/patient-3", null))
        .withRow("encounter-3", RowFactory.create(null, "Patient/patient-2", null))
        .withRow("encounter-4", RowFactory.create(null, "Patient/patient-2", null))
        .withRow("encounter-5", RowFactory.create(null, "Group/group-1", null))
        .buildWithStructValue();
    final IdAndValueColumns idAndValueColumns = getIdAndValueColumns(argumentDatasetPreJoin);
    final Column idColumn = idAndValueColumns.getId();
    final Column valueColumn = idAndValueColumns.getValues().get(0);

    final Dataset<Row> argumentDataset = join(originPath.getDataset(),
        originPath.getIdColumn(), argumentDatasetPreJoin, idColumn, JoinType.LEFT_OUTER);
    final FhirPath argumentPath = new ElementPathBuilder(spark)
        .dataset(argumentDataset)
        .idColumn(originPath.getIdColumn())
        .valueColumn(valueColumn)
        .expression("Encounter.subject")
        .singular(false)
        .foreignResource(originPath)
        .definition(definition)
        .buildDefined();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputPath.getIdColumn())
        .resourceReader(database)
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput reverseResolveInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentPath));
    final NamedFunction reverseResolve = NamedFunction.getInstance("reverseResolve");
    final FhirPath result = reverseResolve.invoke(reverseResolveInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("reverseResolve(Encounter.subject)")
        .isNotSingular()
        .hasResourceType(ResourceType.ENCOUNTER);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withIdColumn()
        .withRow("patient-1", "encounter-1")
        .withRow("patient-2", "encounter-3")
        .withRow("patient-2", "encounter-4")
        .withRow("patient-3", "encounter-2")
        .withRow("patient-4", null)
        .build();
    assertThat(result)
        .selectOrderedResult()
        .hasRows(expectedDataset);
  }

  @Test
  void throwsErrorIfInputNotResource() {
    final ElementPath input = new ElementPathBuilder(spark)
        .expression("gender")
        .fhirType(FHIRDefinedType.CODE)
        .build();
    final ElementPath argument = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.REFERENCE)
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput reverseResolveInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction reverseResolveFunction = NamedFunction.getInstance("reverseResolve");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> reverseResolveFunction.invoke(reverseResolveInput));
    assertEquals(
        "Input to reverseResolve function must be a resource: gender",
        error.getMessage());
  }

  @Test
  void throwsErrorIfArgumentIsNotReference() {
    final ResourcePath input = new ResourcePathBuilder(spark).build();
    final ElementPath argument = new ElementPathBuilder(spark)
        .expression("gender")
        .fhirType(FHIRDefinedType.CODE)
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput reverseResolveInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction reverseResolveFunction = NamedFunction.getInstance("reverseResolve");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> reverseResolveFunction.invoke(reverseResolveInput));
    assertEquals(
        "Argument to reverseResolve function must be a Reference: gender",
        error.getMessage());
  }

  @Test
  void throwsErrorIfMoreThanOneArgument() {
    final ResourcePath input = new ResourcePathBuilder(spark)
        .expression("Patient")
        .build();
    final ElementPath argument1 = new ElementPathBuilder(spark)
        .expression("Encounter.subject")
        .fhirType(FHIRDefinedType.REFERENCE)
        .build();
    final ElementPath argument2 = new ElementPathBuilder(spark)
        .expression("Encounter.participant.individual")
        .fhirType(FHIRDefinedType.REFERENCE)
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput reverseResolveInput = new NamedFunctionInput(parserContext, input,
        Arrays.asList(argument1, argument2));

    final NamedFunction reverseResolveFunction = NamedFunction.getInstance("reverseResolve");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> reverseResolveFunction.invoke(reverseResolveInput));
    assertEquals(
        "reverseResolve function accepts a single argument: reverseResolve(Encounter.subject, Encounter.participant.individual)",
        error.getMessage());
  }

  @Test
  void throwsErrorIfArgumentTypeDoesNotMatchInput() {
    final ResourcePath input = new ResourcePathBuilder(spark)
        .resourceType(ResourceType.PATIENT)
        .expression("Patient")
        .build();
    final BaseRuntimeChildDefinition childDefinition = fhirContext
        .getResourceDefinition("Encounter").getChildByName("episodeOfCare");
    final ElementDefinition definition = ElementDefinition
        .build(childDefinition, "episodeOfCare");
    final ElementPath argument = new ElementPathBuilder(spark)
        .expression("Encounter.episodeOfCare")
        .fhirType(FHIRDefinedType.REFERENCE)
        .definition(definition)
        .buildDefined();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput reverseResolveInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction reverseResolveFunction = NamedFunction.getInstance("reverseResolve");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> reverseResolveFunction.invoke(reverseResolveInput));
    assertEquals(
        "Reference in argument to reverseResolve does not support input resource type: reverseResolve(Encounter.episodeOfCare)",
        error.getMessage());
  }

}