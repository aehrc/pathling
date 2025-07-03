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

package au.csiro.pathling.views;

import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.RegisterExtension;
import java.util.Set;

public class FhirViewShareableComplianceTest extends FhirViewTest {

  @RegisterExtension
  static final Extension JSON_REPORTING_EXTENSION = new JsonReportingExtension(
      "target/fhir-view-compliance-test.json");

  public FhirViewShareableComplianceTest() {
    super("classpath:tests/sql-on-fhir/*.json",
        Set.of("shareable"),
        Set.of(
            "constant_types - date",
            "constant_types - dateTime",
            "constant_types - instant",
            "constant_types - time",
            // TODO: string represention of Decimal values
            "fhirpath_numbers - add observation"
        )
    );
  }
}
