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

package au.csiro.pathling.test.datasource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.layout.PofDatasets;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests a {@link PrunedSchemaReader} that checks the elements named for removal against the FHIR
 * definitions, rather than against a schema that carries every element.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class PrunedSchemaReaderTest {

  @Autowired SparkSession spark;

  @Autowired FhirContext fhirContext;

  @TempDir Path tempDir;

  @Test
  void removesAnElementTheWrittenSchemaCarries() {
    final PrunedSchemaReader reader = observations();
    assertThat(reader.read().schema().fieldNames()).contains("valueQuantity");

    final StructType pruned = reader.readWithout("valueQuantity").schema();

    assertThat(pruned.fieldNames()).doesNotContain("valueQuantity").contains("id", "component");
  }

  @Test
  void leavesTheSchemaAsItIsForADescribedElementTheWrittenSchemaLacks() {
    final PrunedSchemaReader reader = observations();

    // A singular primitive, a top-level choice variant, and a choice variant nested under a
    // repeating parent, none of which the source populates.
    final StructType pruned =
        reader.readWithout("status", "valueString", "component.valueBoolean").schema();

    assertThat(pruned).isEqualTo(reader.read().schema());
  }

  @Test
  void refusesAnElementTheDefinitionsDoNotDescribe() {
    final PrunedSchemaReader reader = observations();

    assertThatThrownBy(() -> reader.readWithout("undescribed"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> reader.readWithout("component.undescribed"))
        .isInstanceOf(IllegalArgumentException.class);
    // A step past a primitive is not described, even though the primitive is.
    assertThatThrownBy(() -> reader.readWithout("status.undescribed"))
        .isInstanceOf(IllegalArgumentException.class);
    // A choice is never stored under its own name, only under the name of each of its variants.
    assertThatThrownBy(() -> reader.readWithout("value"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> reader.readWithout("component.value"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void removesANestedElementTheWrittenSchemaCarries() {
    final PrunedSchemaReader reader = observations();

    final StructType pruned = reader.readWithout("component.valueQuantity").schema();

    final StructType component =
        (StructType) ((ArrayType) pruned.apply("component").dataType()).elementType();
    assertThat(component.fieldNames()).doesNotContain("valueQuantity").contains("code");
  }

  @Nonnull
  private PrunedSchemaReader observations() {
    final String json =
        """
        {"resourceType":"Observation","id":"o1",
         "code":{"text":"c"},
         "valueQuantity":{"value":5,"unit":"mg"},
         "component":[{"code":{"text":"d"},"valueQuantity":{"value":3,"unit":"mg"}}]}
        """
            .replace("\n", "");
    return PrunedSchemaReader.write(
        PofDatasets.fromJson(spark, fhirContext, "Observation", List.of(json)),
        tempDir.resolve("Observation").toString(),
        fhirContext,
        "Observation");
  }
}
