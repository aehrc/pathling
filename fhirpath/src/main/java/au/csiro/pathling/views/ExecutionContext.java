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

package au.csiro.pathling.views;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;

/**
 * The execution context for a FHIR view, which encapsulates the minimalistic set of operations
 * required to evaluate a view.
 */
public interface ExecutionContext {

  /**
   * Creates a new execution context that is a sub-context of this one, with the given path.
   *
   * @param path the path to the sub-context
   * @param unnest whether to unnest the sub-context
   * @return the new sub-context
   */
  @Nonnull
  ExecutionContext subContext(@Nonnull String path, boolean unnest);

  /**
   * Evaluates the given FHIRPath path and returns the result as a column.
   *
   * @param path the path to evaluate
   * @param singularise whether to singularise the result
   * @return the result as a column
   */
  @Nonnull
  Column evalExpression(@Nonnull final String path, final boolean singularise);


  /**
   * Evaluates the given FHIRPath path and returns the result as a column.
   *
   * @param path the path to evaluate
   * @return the result as a column
   */
  @Nonnull
  default Column evalExpression(@Nonnull final String path) {
    return evalExpression(path, true);
  }


  /**
   * Evaluates the given FHIRPath path and returns the result as a column with the given alias.
   *
   * @param path the path to evaluate
   * @param singularise whether to singularise the result
   * @param alias the alias to use for the column
   * @return the result as a column
   */
  @Nonnull
  default Column evalExpression(@Nonnull final String path, final boolean singularise,
      @Nonnull final String alias) {
    return evalExpression(path, singularise).as(alias);
  }

  /**
   * Evaluates the given FHIRPath path and returns the result as a column with the given alias.
   *
   * @param path the path to evaluate
   * @param singularise whether to singularise the result
   * @param maybeAlias the alias to use for the column
   * @return the result as a column
   */
  @Nonnull
  default Column evalExpression(@Nonnull final String path, final boolean singularise,
      @Nonnull final
      Optional<String> maybeAlias) {
    return maybeAlias.map(alias -> evalExpression(path, singularise, alias))
        .orElse(evalExpression(path, singularise));
  }
}
