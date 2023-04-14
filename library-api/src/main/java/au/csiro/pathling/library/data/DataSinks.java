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

package au.csiro.pathling.library.data;

import static au.csiro.pathling.io.PersistenceScheme.departitionResult;
import static au.csiro.pathling.utilities.Strings.safelyJoinPaths;

import au.csiro.pathling.library.FhirMimeTypes;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.EnumerableDataSource;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

public class DataSinks {

  @Nonnull
  private final PathlingContext pathlingContext;

  @Nonnull
  private final EnumerableDataSource dataSource;

  public DataSinks(@Nonnull final PathlingContext pathlingContext,
      @Nonnull final EnumerableDataSource dataSource) {
    this.pathlingContext = pathlingContext;
    this.dataSource = dataSource;
  }

  public void toNdjsonDir(@Nonnull final String ndjsonDir) {
    for (final ResourceType resourceType : dataSource.getDefinedResources()) {
      final Dataset<String> jsonStrings = pathlingContext.decode(dataSource.read(resourceType),
          resourceType.toCode(), FhirMimeTypes.FHIR_JSON);
      final String resultUrl = safelyJoinPaths(ndjsonDir, resourceType.toCode() + ".ndjson");
      final String resultUrlPartitioned = resultUrl + ".partitioned";
      jsonStrings.coalesce(1).write().text(resultUrlPartitioned);
      departitionResult(pathlingContext.getSpark(), resultUrlPartitioned, resultUrl, "txt");
    }
  }

}
