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

package au.csiro.pathling.export.fhir;

import au.csiro.pathling.export.fhir.Parameters.Parameter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParametersTest {


  @Test
  public void testSerializerParametersWithReference() {

    final Parameters parameters = Parameters.builder()
        .parameter(List.of(
            Parameter.builder()
                .name("reference")
                .valueReference(Reference.of("Patient/00"))
                .build(),
            Parameter.builder()
                .name("reference")
                .valueReference(Reference.of("Patient/01"))
                .build()
        ))
        .build();
    assertEquals(
        new JSONObject()
            .put("resourceType", "Parameters")
            .put("parameter",
                new JSONArray()
                    .put(
                        new JSONObject()
                            .put("name", "reference")
                            .put("valueReference", new JSONObject().put("reference", "Patient/00"))
                    )
                    .put(
                        new JSONObject()
                            .put("name", "reference")
                            .put("valueReference", new JSONObject().put("reference", "Patient/01"))
                    )
            ).toString(),
        new JSONObject(JsonSupport.toJson(parameters)).toString()
    );
  }
}