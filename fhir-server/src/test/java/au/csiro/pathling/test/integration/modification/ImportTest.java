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

package au.csiro.pathling.test.integration.modification;

import static au.csiro.pathling.test.TestResources.getResourceAsUrl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.errors.ErrorHandlingInterceptor;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.ImportMode;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.update.ImportExecutor;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
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
class ImportTest extends ModificationTest {

  @Autowired
  SparkSession spark;

  @Autowired
  Database database;

  @Autowired
  ImportExecutor importExecutor;

  @SuppressWarnings("SameParameterValue")
  @Nonnull
  Parameters buildImportParameters(@Nonnull final URL jsonURL,
      @Nonnull final ResourceType resourceType) {
    final Parameters parameters = new Parameters();
    final ParametersParameterComponent sourceParam = parameters.addParameter().setName("source");
    sourceParam.addPart().setName("resourceType").setValue(new CodeType(resourceType.toCode()));
    sourceParam.addPart().setName("url").setValue(new UrlType(jsonURL.toExternalForm()));
    return parameters;
  }

  @SuppressWarnings("SameParameterValue")
  @Nonnull
  Parameters buildImportParameters(@Nonnull final URL jsonURL,
      @Nonnull final ResourceType resourceType, @Nonnull final ImportMode mode) {
    final Parameters parameters = buildImportParameters(jsonURL, resourceType);
    final ParametersParameterComponent sourceParam = parameters.getParameter().stream()
        .filter(p -> p.getName().equals("source")).findFirst()
        .orElseThrow();
    sourceParam.addPart().setName("mode").setValue(new CodeType(mode.getCode()));
    return parameters;
  }

  @Test
  void importJsonFile() {
    final URL jsonURL = getResourceAsUrl("import/Patient.ndjson");
    importExecutor.execute(buildImportParameters(jsonURL, ResourceType.PATIENT));

    final Dataset<Row> result = database.read(ResourceType.PATIENT);
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
  void mergeJsonFile() {
    final URL jsonURL = getResourceAsUrl("import/Patient_updates.ndjson");
    importExecutor.execute(
        buildImportParameters(jsonURL, ResourceType.PATIENT, ImportMode.MERGE));

    final Dataset<Row> result = database.read(ResourceType.PATIENT);
    final Dataset<Row> expected = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("121503c8-9564-4b48-9086-a22df717948e", "2022-01-01")
        .withRow("2b36c1e2-bbe1-45ae-8124-4adad2677702", "2022-01-01")
        .withRow("7001ad9c-34d2-4eb5-8165-5fdc2147f469", "1959-09-27")
        .withRow("8ee183e2-b3c0-4151-be94-b945d6aa8c6d", "2022-01-01")
        .withRow("9360820c-8602-4335-8b50-c88d627a0c20", "2022-01-01")
        .withRow("a7eb2ce7-1075-426c-addd-957b861b0e55", "2022-01-01")
        .withRow("bbd33563-70d9-4f6d-a79a-dd1fc55f5ad9", "2022-01-01")
        .withRow("beff242e-580b-47c0-9844-c1a68c36c5bf", "2022-01-01")
        .withRow("e62e52ae-2d75-4070-a0ae-3cc78d35ed08", "2022-01-01")
        .withRow("foo", "2022-01-01")
        .build();

    DatasetAssert.of(result.select("id", "birthDate")).hasRows(expected);
  }

  @Test
  void importJsonFileWithBlankLines() {
    final URL jsonURL = getResourceAsUrl("import/Patient_with_eol.ndjson");
    importExecutor.execute(buildImportParameters(jsonURL, ResourceType.PATIENT));
    assertEquals(9, database.read(ResourceType.PATIENT).count());
  }

  @Test
  void importJsonFileWithRecursiveDatatype() {
    final URL jsonURL = getResourceAsUrl("import/Questionnaire.ndjson");
    importExecutor.execute(buildImportParameters(jsonURL, ResourceType.QUESTIONNAIRE));
    final Dataset<Row> questionnaireDataset = database.read(ResourceType.QUESTIONNAIRE);
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
      final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
          () -> importExecutor.execute(
              buildImportParameters(new URL("file://some/url"),
                  resourceType)), "Unsupported resource type: " + resourceType.toCode());
      assertEquals("Unsupported resource type: " + resourceType.toCode(), error.getMessage());
    }
  }

  @Test
  void throwsOnMissingId() {
    final URL jsonURL = getResourceAsUrl("import/Patient_missing_id.ndjson");
    final Exception error = assertThrows(Exception.class,
        () -> importExecutor.execute(buildImportParameters(jsonURL, ResourceType.PATIENT)));
    final BaseServerResponseException convertedError =
        ErrorHandlingInterceptor.convertError(error);
    assertTrue(convertedError instanceof InvalidRequestException);
    assertEquals("Encountered a resource with no ID", convertedError.getMessage());
  }

}
