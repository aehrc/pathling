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
package au.csiro.pathling.examples;

import static au.csiro.pathling.views.FhirView.column;
import static au.csiro.pathling.views.FhirView.columns;
import static au.csiro.pathling.views.FhirView.forEach;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.views.FhirView;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PathlingJavaApp {

  public static void main(String[] args) {
    PathlingContext pc = PathlingContext.create();
    final QueryableDataSource data = pc.read().ndjson("/tmp/ndjson");

    final FhirView view =
        FhirView.ofResource("Observation")
            .select(
                columns(column("patient_id", "getResourceKey()")),
                forEach(
                    "code.coding",
                    column("code_system", "system"),
                    column("code_code", "code"),
                    column("code_display", "display")))
            .build();

    final Dataset<Row> result = data.view(view).execute();

    result.show(false);
  }
}
