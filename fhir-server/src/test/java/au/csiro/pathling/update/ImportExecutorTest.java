/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.helpers.TestHelpers;
import au.csiro.pathling.test.integration.modification.ModificationTest;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

/**
 * @author John Grimes
 */
@TestPropertySource(
    properties = {
        "pathling.storage.databaseName=default",
        "pathling.import.allowableSources=file:/",
        "pathling.encoding.maxNestingLevel=5"
    })
class ImportExecutorTest extends ModificationTest {

  @Autowired
  private SparkSession spark;

  @Autowired
  private ResourceReader resourceReader;

  @Autowired
  private ImportExecutor importExecutor;

  @SuppressWarnings("SameParameterValue")
  @Nonnull
  private Parameters buildImportParameters(@Nonnull final URL jsonURL,
      @Nonnull final ResourceType resourceType) {
    return buildImportParameters(jsonURL, resourceType, false);
  }

  @SuppressWarnings("SameParameterValue")
  @Nonnull
  private Parameters buildImportParameters(@Nonnull final URL jsonURL,
      @Nonnull final ResourceType resourceType, final boolean generateIds) {
    final Parameters parameters = new Parameters();
    final ParametersParameterComponent sourceParam = parameters.addParameter().setName("source");
    sourceParam.addPart().setName("resourceType").setValue(new CodeType(resourceType.toCode()));
    sourceParam.addPart().setName("url").setValue(new UrlType(jsonURL.toExternalForm()));
    if (generateIds) {
      final ParametersParameterComponent generateIdsParam = parameters.addParameter()
          .setName("generateIDs");
      generateIdsParam.setValue(new BooleanType(true));
    }
    return parameters;
  }

  @Test
  public void importJsonFile() {
    final URL jsonURL = TestHelpers.getResourceAsUrl("import/Patient.ndjson");
    importExecutor.execute(buildImportParameters(jsonURL, ResourceType.PATIENT));

    final Dataset<Row> result = resourceReader.read(ResourceType.PATIENT);
    final Dataset<Row> expected = new DatasetBuilder(spark)
        .withIdColumn()
        .withRow("121503c8-9564-4b48-9086-a22df717948e")
        .withRow("2b36c1e2-bbe1-45ae-8124-4adad2677702")
        .withRow("7001ad9c-34d2-4eb5-8165-5fdc2147f469")
        .withRow("8ee183e2-b3c0-4151-be94-b945d6aa8c6d")
        .withRow("9360820c-8602-4335-8b50-c88d627a0c20")
        .withRow("a7eb2ce7-1075-426c-addd-957b861b0e55")
        .withRow("bbd33563-70d9-4f6d-a79a-dd1fc55f5ad9")
        .withRow("beff242e-580b-47c0-9844-c1a68c36c5bf")
        .withRow("e62e52ae-2d75-4070-a0ae-3cc78d35ed08")
        .build();

    DatasetAssert.of(result.select("id")).hasRows(expected);
  }

  @Test
  public void importJsonFileWithGeneratedIds() {
    final URL jsonURL = TestHelpers.getResourceAsUrl("import/Patient.ndjson");
    importExecutor.execute(buildImportParameters(jsonURL, ResourceType.PATIENT, true));

    final Dataset<Row> result = resourceReader.read(ResourceType.PATIENT);
    final Dataset<Row> expected = new DatasetBuilder(spark)
        .withIdColumn()
        .withRow("121503c8-9564-4b48-9086-a22df717948e")
        .withRow("2b36c1e2-bbe1-45ae-8124-4adad2677702")
        .withRow("7001ad9c-34d2-4eb5-8165-5fdc2147f469")
        .withRow("8ee183e2-b3c0-4151-be94-b945d6aa8c6d")
        .withRow("9360820c-8602-4335-8b50-c88d627a0c20")
        .withRow("a7eb2ce7-1075-426c-addd-957b861b0e55")
        .withRow("bbd33563-70d9-4f6d-a79a-dd1fc55f5ad9")
        .withRow("beff242e-580b-47c0-9844-c1a68c36c5bf")
        .withRow("e62e52ae-2d75-4070-a0ae-3cc78d35ed08")
        .build();

    DatasetAssert.of(result.select("id")).rowsAreAllNotEqual(expected);
  }

  @Test
  public void importJsonFileWithBlankLines() {
    final URL jsonURL = TestHelpers.getResourceAsUrl("import/Patient_with_eol.ndjson");
    importExecutor.execute(buildImportParameters(jsonURL, ResourceType.PATIENT));
    assertEquals(9, resourceReader.read(ResourceType.PATIENT).count());
  }

  @Test
  public void importJsonFileWithRecursiveDatatype() {
    final URL jsonURL = TestHelpers.getResourceAsUrl("import/Questionnaire.ndjson");
    importExecutor.execute(buildImportParameters(jsonURL, ResourceType.QUESTIONNAIRE));
    final Dataset<Row> questionnaireDataset = resourceReader.read(ResourceType.QUESTIONNAIRE);
    assertEquals(1, questionnaireDataset.count());

    final Dataset<Row> expandedItemsDataset = questionnaireDataset
        .withColumn("item_l0", functions.explode_outer(functions.col("item")))
        .withColumn("item_l1", functions.explode_outer(functions.col("item_l0").getField("item")))
        .withColumn("item_l2", functions.explode_outer(functions.col("item_l1").getField("item")))
        .withColumn("item_l3", functions.explode_outer(functions.col("item_l2").getField("item")))
        .withColumn("item_l4", functions.explode_outer(functions.col("item_l3").getField("item")))
        .withColumn("item_l5", functions.explode_outer(functions.col("item_l4").getField("item")))
        .select(
            functions.col("item_l0").getField("linkId"),
            functions.col("item_l1").getField("linkId"),
            functions.col("item_l2").getField("linkId"),
            functions.col("item_l3").getField("linkId"),
            functions.col("item_l4").getField("linkId"),
            functions.col("item_l5").getField("linkId")
        );

    // The actual data has maxNestingLevel = 5, but we import with maxNestingLevel == 5
    // So we expect level 5 items to be NULL
    final List<Row> expectedDataset = Arrays.asList(
        RowFactory.create("1", "1.1", "1.1.1", "1.1.1.1", "1.1.1.1.1", null),
        RowFactory.create("1", "1.1", "1.1.1", "1.1.1.1", "1.1.1.1.2", null),
        RowFactory.create("1", "1.1", "1.1.1", "1.1.1.2", null, null),
        RowFactory.create("2", "2.1", "2.1.2", null, null, null)
    );

    DatasetAssert.of(expandedItemsDataset).hasRows(expectedDataset);
  }

  @Test
  void throwsOnUnsupportedResourceType() {
    final List<ResourceType> resourceTypes = Arrays.asList(ResourceType.PARAMETERS,
        ResourceType.TASK, ResourceType.STRUCTUREDEFINITION, ResourceType.STRUCTUREMAP,
        ResourceType.BUNDLE);
    for (final ResourceType resourceType : resourceTypes) {
      assertThrows(InvalidUserInputError.class, () -> importExecutor.execute(
          buildImportParameters(new URL("file://some/url"),
              resourceType)), "Unsupported resource type: " + resourceType.toCode());
    }
  }

}
