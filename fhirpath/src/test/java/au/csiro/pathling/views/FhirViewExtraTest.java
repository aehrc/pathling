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

import java.util.Set;

public class FhirViewExtraTest extends FhirViewTest {

  public FhirViewExtraTest() {
    super(
        "classpath:viewTests/*.json",
        Set.of(),
        // The forEach typed-empty fallback is not yet implemented; see issue #2625.
        Set.of(
            "deep nesting - forEach on a recursive path that exits the schema",
            "deep nesting - forEach on a recursive path that exits the schema with sibling"
                + " column"));
  }
}
