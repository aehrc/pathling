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

package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.parser.Parser;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public interface FhirPathExecutor {

  @Nonnull
  Collection validate(@Nonnull final FhirPath path);

  @Nonnull
  Dataset<Row> execute(@Nonnull final FhirPath path);

  @Nonnull
  CollectionDataset evaluate(@Nonnull final FhirPath path);

  @Nonnull
  default CollectionDataset evaluate(@Nonnull final String fhirpathExpression) {
    return evaluate(new Parser().parse(fhirpathExpression));
  }

  @Nonnull
  default Dataset<Row> execute(@Nonnull final String expression) {
    return execute(new Parser().parse(expression));
  }

  @Nonnull
  Collection evaluate(@Nonnull final FhirPath path, @Nonnull final Collection inputContext);

  @Nonnull
  Collection createDefaultInputContext();

  @Nonnull
  Dataset<Row> createInitialDataset();

}
