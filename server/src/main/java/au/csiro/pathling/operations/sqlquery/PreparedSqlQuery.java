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

package au.csiro.pathling.operations.sqlquery;

import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import java.util.Map;
import lombok.Value;

/**
 * A SQL query that has been parsed and had its ViewDefinition table sources resolved, ready for
 * static validation and execution by {@link SqlQueryPipeline}. Produced by {@link
 * SqlQueryPipeline#prepare}; shared by the synchronous {@code $sqlquery-run} and the asynchronous
 * {@code $sqlquery-export} operations.
 */
@Value
public class PreparedSqlQuery {

  /** The validated, normalised request: parsed query, output format, header flag, bindings. */
  @Nonnull SqlQueryRequest request;

  /** The resolved view table sources the SQL references, keyed by table label. */
  @Nonnull Map<String, FhirView> resolvedViews;
}
