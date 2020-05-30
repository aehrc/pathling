/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.assertions.Assertions.assertTrue;
import static au.csiro.pathling.test.helpers.SparkHelpers.getIdAndValueColumns;
import static au.csiro.pathling.test.helpers.SparkHelpers.referenceStructType;
import static au.csiro.pathling.test.helpers.TestHelpers.mockAvailableResourceTypes;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.TestElementPath;
import au.csiro.pathling.test.TestParserContext;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.SparkHelpers.IdAndValueColumns;
import java.util.Collections;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class ResolveFunctionTest {

  private ResourceReader mockReader;

  @BeforeEach
  void setUp() {
    mockReader = mock(ResourceReader.class);
  }

  @Test
  public void simpleResolve() {
    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource("Encounter", "episodeOfCare");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> referenceDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(referenceStructType())
        .withRow("Encounter/xyz1", RowFactory.create(null, "EpisodeOfCare/abc1", null))
        .withRow("Encounter/xyz2", RowFactory.create(null, "EpisodeOfCare/abc3", null))
        .withRow("Encounter/xyz3", RowFactory.create(null, "EpisodeOfCare/abc2", null))
        .withRow("Encounter/xyz4", RowFactory.create(null, "EpisodeOfCare/abc2", null))
        .buildWithStructValue();
    final FhirPath referencePath = TestElementPath
        .build("Encounter.episodeOfCare", referenceDataset, false, definition);

    final Dataset<Row> episodeOfCareDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("status", DataTypes.StringType)
        .withRow("EpisodeOfCare/abc1", "planned")
        .withRow("EpisodeOfCare/abc2", "waitlist")
        .withRow("EpisodeOfCare/abc3", "active")
        .build();
    when(mockReader.read(ResourceType.EPISODEOFCARE)).thenReturn(episodeOfCareDataset);

    final NamedFunctionInput resolveInput = buildFunctionInput(referencePath);
    final FhirPath result = invokeResolve(resolveInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("Encounter.episodeOfCare.resolve()")
        .isNotSingular()
        .hasResourceType(ResourceType.EPISODEOFCARE);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("status", DataTypes.StringType)
        .withRow("Encounter/xyz1", RowFactory.create("EpisodeOfCare/abc1", "planned"))
        .withRow("Encounter/xyz2", RowFactory.create("EpisodeOfCare/abc3", "active"))
        .withRow("Encounter/xyz3", RowFactory.create("EpisodeOfCare/abc2", "waitlist"))
        .withRow("Encounter/xyz4", RowFactory.create("EpisodeOfCare/abc2", "waitlist"))
        .buildWithStructValue();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void polymorphicResolve() {
    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource("Encounter", "subject");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> referenceDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(referenceStructType())
        .withRow("Encounter/xyz1", RowFactory.create(null, "Patient/abc1", null))
        .withRow("Encounter/xyz2", RowFactory.create(null, "Patient/abc3", null))
        .withRow("Encounter/xyz3", RowFactory.create(null, "Patient/abc2", null))
        .withRow("Encounter/xyz4", RowFactory.create(null, "Patient/abc2", null))
        .withRow("Encounter/xyz5", RowFactory.create(null, "Group/def1", null))
        .buildWithStructValue();
    final FhirPath referencePath = TestElementPath
        .build("Encounter.subject", referenceDataset, true, definition);

    final Dataset<Row> patientDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("Patient/abc1", "female", true)
        .withRow("Patient/abc2", "female", false)
        .withRow("Patient/abc3", "male", true)
        .build();
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);

    final Dataset<Row> groupDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("name", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("Group/def1", "Some group", true)
        .build();
    when(mockReader.read(ResourceType.GROUP))
        .thenReturn(groupDataset);

    mockAvailableResourceTypes(mockReader, ResourceType.PATIENT, ResourceType.GROUP);

    final NamedFunctionInput resolveInput = buildFunctionInput(referencePath);
    final FhirPath result = invokeResolve(resolveInput);

    assertTrue(result instanceof UntypedResourcePath);
    assertThat((UntypedResourcePath) result)
        .hasExpression("Encounter.subject.resolve()")
        .isSingular()
        .hasPossibleTypes(ResourceType.PATIENT, ResourceType.GROUP);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withTypeColumn()
        .withStructTypeColumns(referenceStructType())
        .withRow("Encounter/xyz1", "Patient", RowFactory.create(null, "Patient/abc1", null))
        .withRow("Encounter/xyz2", "Patient", RowFactory.create(null, "Patient/abc3", null))
        .withRow("Encounter/xyz3", "Patient", RowFactory.create(null, "Patient/abc2", null))
        .withRow("Encounter/xyz4", "Patient", RowFactory.create(null, "Patient/abc2", null))
        .withRow("Encounter/xyz5", "Group", RowFactory.create(null, "Group/def1", null))
        .buildWithStructValue();
    assertThat((UntypedResourcePath) result)
        .selectUntypedResourceResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void polymorphicResolveAnyType() {
    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource("Condition", "evidence")
        .flatMap(child -> child.getChildElement("detail"));
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> referenceDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(referenceStructType())
        .withRow("Condition/xyz1", RowFactory.create(null, "Observation/abc1", null))
        .withRow("Condition/xyz2", RowFactory.create(null, "ClinicalImpression/def1", null))
        .buildWithStructValue();
    final FhirPath referencePath = TestElementPath
        .build("Condition.evidence.detail", referenceDataset, false, definition);

    final Dataset<Row> observationDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("status", DataTypes.StringType)
        .withRow("Observation/abc1", "registered")
        .build();
    when(mockReader.read(ResourceType.OBSERVATION))
        .thenReturn(observationDataset);

    final Dataset<Row> clinicalImpressionDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("status", DataTypes.StringType)
        .withRow("ClinicalImpression/def1", "in-progress")
        .build();
    when(mockReader.read(ResourceType.CLINICALIMPRESSION))
        .thenReturn(clinicalImpressionDataset);

    mockAvailableResourceTypes(mockReader, ResourceType.OBSERVATION,
        ResourceType.CLINICALIMPRESSION);

    final NamedFunctionInput resolveInput = buildFunctionInput(referencePath);
    final FhirPath result = invokeResolve(resolveInput);

    assertTrue(result instanceof UntypedResourcePath);
    assertThat((UntypedResourcePath) result)
        .hasExpression("Condition.evidence.detail.resolve()")
        .isNotSingular()
        .hasPossibleTypes(ResourceType.OBSERVATION, ResourceType.CLINICALIMPRESSION);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withTypeColumn()
        .withStructTypeColumns(referenceStructType())
        .withRow("Condition/xyz1", "Observation", RowFactory.create(null, "Observation/abc1", null))
        .withRow("Condition/xyz2", "ClinicalImpression",
            RowFactory.create(null, "ClinicalImpression/def1", null))
        .buildWithStructValue();
    assertThat((UntypedResourcePath) result)
        .selectUntypedResourceResult()
        .hasRows(expectedDataset);
  }


  @Test
  public void throwExceptionWhenInputNotReference() {
    final Dataset<Row> patientDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .build();
    final IdAndValueColumns columns = getIdAndValueColumns(patientDataset);
    final FhirPath genderPath = new StringPath("Patient.gender", patientDataset, columns.getId(),
        columns.getValue(), true,
        FHIRDefinedType.CODE);

    final NamedFunctionInput resolveInput = buildFunctionInput(genderPath);

    assertThrows(InvalidUserInputError.class, () -> invokeResolve(resolveInput),
        "Input to resolve function must be a Reference: gender");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void throwExceptionWhenArgumentSupplied() {
    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource("Encounter", "episodeOfCare");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> referenceDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(referenceStructType())
        .buildWithStructValue();
    final FhirPath referencePath = TestElementPath
        .build("Encounter.episodeOfCare", referenceDataset, false, definition);

    final FhirPath inputContext = mock(FhirPath.class);
    when(inputContext.getDataset()).thenReturn(mock(Dataset.class));
    final ParserContext parserContext = TestParserContext.build(inputContext, mockReader);
    final StringLiteralPath stringLiteralPath = StringLiteralPath
        .fromString("'foo'", parserContext.getInputContext());
    final NamedFunctionInput resolveInput = new NamedFunctionInput(parserContext, referencePath,
        Collections.singletonList(stringLiteralPath));

    assertThrows(InvalidUserInputError.class, () -> invokeResolve(resolveInput),
        "resolve function does not accept arguments");
  }

  @Nonnull
  private NamedFunctionInput buildFunctionInput(@Nonnull final FhirPath inputPath) {
    final ParserContext parserContext = TestParserContext
        .build(inputPath.getIdColumn(), mockReader);
    return new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
  }

  @Nonnull
  private FhirPath invokeResolve(@Nonnull final NamedFunctionInput resolveInput) {
    final NamedFunction resolve = NamedFunction.getInstance("resolve");
    return resolve.invoke(resolveInput);
  }

}