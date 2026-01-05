/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests for {@link ResourceCollection}.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
class ResourceCollectionTest {

  @Autowired SparkSession spark;

  @Autowired FhirContext fhirContext;

  @Test
  void buildWithResourceTypeEnum() {
    // Test the build() overload that takes a ResourceType enum.
    final DefaultRepresentation column = new DefaultRepresentation(functions.col("Patient"));

    final ResourceCollection collection =
        ResourceCollection.build(column, fhirContext, ResourceType.PATIENT);

    assertNotNull(collection);
    assertEquals("Patient", collection.getResourceDefinition().getResourceCode());
    // Verify that getFhirType(ResourceType) was called and returned a valid type.
    assertTrue(collection.getFhirType().isPresent());
  }

  @Test
  void buildWithResourceTypeEnumObservation() {
    // Test with a different resource type to ensure the enum handling works generally.
    final DefaultRepresentation column = new DefaultRepresentation(functions.col("Observation"));

    final ResourceCollection collection =
        ResourceCollection.build(column, fhirContext, ResourceType.OBSERVATION);

    assertNotNull(collection);
    assertEquals("Observation", collection.getResourceDefinition().getResourceCode());
    assertTrue(collection.getFhirType().isPresent());
  }

  @Test
  void buildWithStringResourceCode() {
    // Test the build() overload that takes a String resource code.
    final DefaultRepresentation column = new DefaultRepresentation(functions.col("Condition"));

    final ResourceCollection collection =
        ResourceCollection.build(column, fhirContext, "Condition");

    assertNotNull(collection);
    assertEquals("Condition", collection.getResourceDefinition().getResourceCode());
    assertTrue(collection.getFhirType().isPresent());
  }
}
