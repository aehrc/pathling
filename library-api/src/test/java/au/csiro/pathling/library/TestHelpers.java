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

package au.csiro.pathling.library;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

public class TestHelpers {

  private TestHelpers() {
    // Utility class - prevent instantiation
  }

  @Nonnull
  public static SparkSession spark() {
    return sparkBuilder().getOrCreate();
  }

  @Nonnull
  public static Builder sparkBuilder() {
    return SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
  }
}
