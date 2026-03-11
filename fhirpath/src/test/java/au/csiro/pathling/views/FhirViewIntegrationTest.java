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

package au.csiro.pathling.views;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import ca.uhn.fhir.context.FhirContext;
import com.google.gson.Gson;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Integration tests that verify physical row data integrity when collecting view results using
 * {@code collectAsList()}. These tests complement the JSON-based {@link FhirViewExtraTest} by
 * exercising the actual collection path, which can expose Spark Catalyst optimizer issues that
 * DataFrame-level comparisons (resolving columns by ExprId) would not detect.
 *
 * @see <a href="https://github.com/aehrc/pathling/issues/2568">Issue #2568</a>
 */
@SpringBootUnitTest
class FhirViewIntegrationTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Autowired FhirContext fhirContext;

  @Autowired Gson gson;

  @Test
  void testForEachCodingWithSiblingTextColumn() {
    final Condition cond1 = new Condition();
    cond1.setId("Condition/cond1");
    cond1.setCode(
        new CodeableConcept()
            .setText("Headache disorder")
            .addCoding(new Coding("http://snomed.info/sct", "25064002", "Headache"))
            .addCoding(new Coding("http://hl7.org/fhir/sid/icd-10", "R51", "Headache")));

    final Condition cond2 = new Condition();
    cond2.setId("Condition/cond2");
    cond2.setCode(
        new CodeableConcept()
            .setText("Diabetes mellitus")
            .addCoding(new Coding("http://snomed.info/sct", "73211009", "Diabetes mellitus")));

    final ObjectDataSource dataSource =
        new ObjectDataSource(spark, fhirEncoders, List.of(cond1, cond2));
    final FhirViewExecutor executor = new FhirViewExecutor(fhirContext, dataSource);

    // ViewDefinition: id + code.text at resource level, forEach code.coding with
    // system/code/display.
    final String viewJson =
        """
        {
          "resource": "Condition",
          "select": [
            {
              "column": [
                {"path": "id", "name": "id"},
                {"path": "code.text", "name": "code_text"}
              ],
              "select": [
                {
                  "forEach": "code.coding",
                  "column": [
                    {"path": "system", "name": "system"},
                    {"path": "code", "name": "code"},
                    {"path": "display", "name": "display"}
                  ]
                }
              ]
            }
          ]
        }
        """;

    final FhirView view = gson.fromJson(viewJson, FhirView.class);
    final Dataset<Row> result = executor.buildQuery(view);

    // Verify collected rows match expected data exactly. Using hasRows() (which calls
    // collectAsList()) exercises the physical collection path where the bug manifested.
    DatasetAssert.of(result)
        .hasRows(
            RowFactory.create(
                "cond1", "Headache disorder", "http://snomed.info/sct", "25064002", "Headache"),
            RowFactory.create(
                "cond1", "Headache disorder", "http://hl7.org/fhir/sid/icd-10", "R51", "Headache"),
            RowFactory.create(
                "cond2",
                "Diabetes mellitus",
                "http://snomed.info/sct",
                "73211009",
                "Diabetes mellitus"));
  }
}
