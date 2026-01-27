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

package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import jakarta.annotation.Nonnull;
import java.util.function.Function;

/**
 * Interface for building DatasetEvaluator instances with specific context.
 *
 * <p>This interface abstracts the creation of evaluators from factories, allowing for caching and
 * other optimizations.
 */
@FunctionalInterface
public interface ResolverBuilder {

  /**
   * Creates a DatasetEvaluator from the given factory function.
   *
   * @param evaluatorFactory factory function that creates a DatasetEvaluator from RuntimeContext
   * @return the created DatasetEvaluator
   */
  @Nonnull
  DatasetEvaluator create(@Nonnull Function<RuntimeContext, DatasetEvaluator> evaluatorFactory);
}
