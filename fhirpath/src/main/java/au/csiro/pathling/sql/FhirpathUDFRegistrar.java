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

package au.csiro.pathling.sql;

import au.csiro.pathling.spark.SparkConfigurer;
import au.csiro.pathling.sql.misc.MiscUDFRegistrar;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.SparkSession;

/**
 * Registration of all fhirpath UDFs.
 */
public class FhirpathUDFRegistrar implements SparkConfigurer {

  private final List<SparkConfigurer> children = List.of(
      new MiscUDFRegistrar()
  );

  @Override
  public void configure(@Nonnull final SparkSession spark) {
    children.forEach(child -> child.configure(spark));
  }

  public static void registerUDFs(@Nonnull final SparkSession spark) {
    new FhirpathUDFRegistrar().configure(spark);
  }
}
