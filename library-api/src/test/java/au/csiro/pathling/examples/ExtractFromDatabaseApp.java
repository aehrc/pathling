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
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

public class ExtractFromDatabaseApp {


  public static void main(final String[] args) {

    final SparkSession spark = SparkSession.builder()
        .appName(ExtractFromJsonApp.class.getName())
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate();

    final PathlingContext ptc = PathlingContext.create(spark);

    final QueryableDataSource data = ptc.read()
        .delta("fhirpath/src/test/resources/test-data/parquet");

    final Dataset<Row> patientResult = data.extract(ResourceType.PATIENT)
        .column("id")
        .column("gender")
        .column("reverseResolve(Condition.subject).code.coding.code")
        .filter("gender = 'male'")
        .execute()
        .limit(5);

    patientResult.show(5);

    final Dataset<Row> conditionResult = data.extract(ResourceType.CONDITION)
        .column("id")
        .column("code.coding.code", "code")
        .column("code.coding.display", "display_name")
        .column("subject.resolve().ofType(Patient).gender", "patient_gender")
        .execute()
        .limit(5);

    conditionResult.show(5);
  }
}
