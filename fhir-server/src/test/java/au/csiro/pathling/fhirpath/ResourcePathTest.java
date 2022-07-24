/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import ca.uhn.fhir.context.FhirContext;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests some basic ResourcePath behaviour.
 *
 * @author Piotr Szul
 */
@SpringBootTest
@Tag("UnitTest")
class ResourcePathTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @MockBean
  Database database;

  @Test
  void adoptFailsToAcceptDatasetWithMissingColumns() {
    final Dataset<Row> rawDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withRow("patient-1", "female")
        .withRow("patient-2", null)
        .build();
    when(database.read(ResourceType.PATIENT)).thenReturn(rawDataset);
    final ResourcePath resourcePath = new ResourcePathBuilder(spark)
        .fhirContext(fhirContext)
        .resourceType(ResourceType.PATIENT)
        .database(database)
        .singular(true)
        .build();

    assertThrows(AssertionError.class, () ->
        resourcePath.adoptDataset(resourcePath.getDataset().select(resourcePath.getIdColumn()))
    );
  }

  @Test
  void adoptAcceptsDatasetWithAdditionalColumns() {
    final Dataset<Row> rawDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withRow("patient-1", "female")
        .withRow("patient-2", null)
        .build();
    when(database.read(ResourceType.PATIENT)).thenReturn(rawDataset);
    final ResourcePath resourcePath = new ResourcePathBuilder(spark)
        .fhirContext(fhirContext)
        .resourceType(ResourceType.PATIENT)
        .database(database)
        .singular(true)
        .build();
    final ResourcePath newPath = resourcePath.adoptDataset(
        resourcePath.getDataset().withColumn("added", functions.lit(0)));

    assertEquals(resourcePath.getIdColumn(), newPath.getIdColumn());
    assertEquals(resourcePath.getValueColumn(), newPath.getValueColumn());
    assertEquals(resourcePath.getThisColumn(), newPath.getThisColumn());
    assertEquals(resourcePath.getEidColumn(), newPath.getEidColumn());
    assertEquals(resourcePath.getResourceType(), newPath.getResourceType());
    assertEquals(resourcePath.getElementColumn("gender"), newPath.getElementColumn("gender"));
    assertEquals(Optional.of(newPath), newPath.getCurrentResource());

  }
}
