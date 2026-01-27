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
import java.util.Map;
import java.util.function.Function;
import lombok.Value;

/**
 * A ResolverBuilder implementation that caches created DatasetEvaluator instances.
 *
 * <p>This builder wraps a delegate ResolverBuilder and maintains a cache of evaluators keyed by
 * their factory functions. This avoids redundant creation of evaluators when the same factory is
 * used multiple times.
 */
@Value(staticConstructor = "of")
public class CachingResolverBuilder implements ResolverBuilder {

  @Nonnull ResolverBuilder delegate;

  @Nonnull Map<Function<RuntimeContext, DatasetEvaluator>, DatasetEvaluator> cache;

  @Override
  @Nonnull
  public DatasetEvaluator create(
      @Nonnull final Function<RuntimeContext, DatasetEvaluator> evaluatorFactory) {
    return cache.computeIfAbsent(evaluatorFactory, delegate::create);
  }
}
