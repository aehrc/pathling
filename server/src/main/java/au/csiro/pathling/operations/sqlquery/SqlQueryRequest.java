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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import lombok.Value;

/**
 * The validated, normalised inputs to a single {@code $sqlquery-run} invocation. Produced by {@link
 * SqlQueryRequestParser} from the raw HTTP-level parameters and consumed by the downstream resolver
 * / executor / streamer pipeline.
 */
@Value
public class SqlQueryRequest {

  /** The decoded SQLQuery Library: SQL text, view references, declared parameters. */
  @Nonnull ParsedSqlQuery parsedQuery;

  /** The selected output format, after applying {@code _format} / {@code Accept} fallback. */
  @Nonnull SqlQueryOutputFormat outputFormat;

  /** Whether to include a header row when emitting CSV. */
  boolean includeHeader;

  /** Optional row cap; {@code null} means no cap. */
  @Nullable Integer limit;

  /**
   * Runtime parameter bindings, typed against the declarations in {@code Library.parameter}. Keys
   * are the parameter names; values are typed Java objects ready to hand to Spark's parameterised
   * SQL API.
   */
  @Nonnull Map<String, Object> parameterBindings;
}
