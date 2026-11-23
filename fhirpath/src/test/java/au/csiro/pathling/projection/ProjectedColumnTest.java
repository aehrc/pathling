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

package au.csiro.pathling.projection;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import com.google.gson.Gson;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests the check of a column's declared FHIR type against the type of its path.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
class ProjectedColumnTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Autowired Gson gson;

  @Test
  void declaredTypeMismatchThrowsInvalidUserInputError() {
    // The view from #2870: meta.profile is a canonical, but the column declares string. The error
    // must be typed, so that callers such as the server can report it as a user error, and must
    // name the column, its path and both types.
    final Patient patient = new Patient();
    patient.setId("p1");
    patient.getMeta().addProfile("http://example.org/StructureDefinition/patient");
    final FhirViewExecutor executor =
        new FhirViewExecutor(
            fhirEncoders.getContext(), new ObjectDataSource(spark, fhirEncoders, List.of(patient)));
    final FhirView view =
        gson.fromJson(
            """
            {
              "resourceType": "ViewDefinition",
              "status": "active",
              "resource": "Patient",
              "select": [
                {
                  "forEachOrNull": "meta.profile",
                  "column": [ { "name": "profile", "path": "$this", "type": "string" } ]
                }
              ]
            }
            """,
            FhirView.class);

    assertThatThrownBy(() -> executor.buildQuery(view))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessage(
            "Column 'profile' with path '$this' has type canonical, but the column declares type "
                + "string");
  }
}
