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

package au.csiro.pathling.view;

import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import lombok.Value;
import org.apache.spark.sql.SparkSession;

/**
 * Dependencies for the execution of a FHIR view.
 */
@Value
public class ExecutionContext {

  /**
   * The {@link SparkSession} for building and executing Spark queries.
   */
  SparkSession spark;

  /**
   * The {@link FhirContext} for accessing the FHIR object model.
   */
  FhirContext fhirContext;

  /**
   * A {@link DataSource} for the FHIR data that the view will be executed against.
   */
  DataSource dataSource;
 
}