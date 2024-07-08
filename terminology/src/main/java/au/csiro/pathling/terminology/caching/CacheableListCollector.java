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

package au.csiro.pathling.terminology.caching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * A collector that collects a stream of items into a list.
 * <p>
 * This is the same as {@link java.util.stream.Collectors#toList()} except that it returns an
 * {@link ArrayList} instead of a generic {@link java.util.List}.
 * <p>
 * We need this because the list implementation needs to be constrained to ensure that it is
 * serializable for persistent caching purposes.
 *
 * @param <T> the type of the elements to collect
 */
public class CacheableListCollector<T> implements Collector<T, ArrayList<T>, ArrayList<T>> {

  @Override
  public Supplier<ArrayList<T>> supplier() {
    return ArrayList::new;
  }

  @Override
  public BiConsumer<ArrayList<T>, T> accumulator() {
    return ArrayList::add;
  }

  @Override
  public BinaryOperator<ArrayList<T>> combiner() {
    return (left, right) -> {
      left.addAll(right);
      return left;
    };
  }

  @Override
  public Function<ArrayList<T>, ArrayList<T>> finisher() {
    return Function.identity();
  }

  @Override
  public Set<Characteristics> characteristics() {
    return Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH));
  }

}
