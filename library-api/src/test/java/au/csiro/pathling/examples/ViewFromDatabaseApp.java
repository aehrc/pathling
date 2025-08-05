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

package au.csiro.pathling.examples;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ViewFromDatabaseApp {

  public static final String VIEW_JSON = """
          {
            "resource": "Patient",
            "select": [
              {
                "column": [
                  {
                    "path": "id",
                    "name": "id"
                  },
                  {
                    "path": "gender",
                    "name": "gender"
                  },
                  {
                    "path": "telecom.where(system='phone').value",
                    "name": "phone_numbers",
                    "collection": true
                  }
                ]
              },
              {
                "forEach": "name",
                "column": [
                  {
                    "path": "use",
                    "name": "name_use"
                  },
                  {
                    "path": "family",
                    "name": "family_name"
                  }
                ],
                "select": [
                  {
                    "forEachOrNull": "given",
                    "column": [
                      {
                        "path": "$this",
                        "nameG": "given_name"
                      }
                    ]
                  }
                ]
              }
            ],
            "where": [
              {
                "path": "gender = 'male'"
              }
            ]
          }
      """;

  public static void main(final String[] args) {

    final SparkSession spark = SparkSession.builder()
        .appName(ViewFromDatabaseApp.class.getName())
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate();

    final PathlingContext ptc = PathlingContext.create(spark);

    final QueryableDataSource data = ptc.read()
        .delta("fhirpath/src/test/resources/test-data/parquet");

    final Dataset<Row> viewResults = data.view("Patient")
        .json(VIEW_JSON)
        .execute()
        .limit(5);

    viewResults.show(5);
  }
}
