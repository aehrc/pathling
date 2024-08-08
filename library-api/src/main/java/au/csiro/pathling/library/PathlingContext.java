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

package au.csiro.pathling.library;

import ca.uhn.fhir.context.FhirVersionEnum;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import scala.compat.java8.OptionConverters;

/**
 * A class designed to provide access to selected Pathling functionality from a language library
 * context.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@Slf4j
@Getter
public class PathlingContext {

  private final SparkSession spark;
  private final FhirVersionEnum fhirVersion;

  public PathlingContext(final SparkSession spark, final String fhirVersion) {
    this.spark = spark;
    this.fhirVersion = FhirVersionEnum.forVersionString(fhirVersion);
  }

  public PathlingContext(final String fhirVersion) {
    this(OptionConverters.toJava(SparkSession.getActiveSession())
        .orElseThrow(() -> new IllegalStateException("No active Spark session")), fhirVersion);
  }

}
