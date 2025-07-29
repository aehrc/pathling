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

import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.TranslationList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * A collector that collects a stream of items into a {@link TranslationList}.
 * <p>
 * This is the same as {@link java.util.stream.Collectors#toList()} except that it returns an
 * {@link TranslationList} instead of a generic {@link java.util.List}.
 * <p>
 * We need this because the list implementation needs to be constrained to ensure that it is
 * serializable for persistent caching purposes.
 */
public class TranslationListCollector implements Collector<Translation, TranslationList, TranslationList> {

  @Override
  public Supplier<TranslationList> supplier() {
    return TranslationList::new;
  }

  @Override
  public BiConsumer<TranslationList, Translation> accumulator() {
    return TranslationList::add;
  }

  @Override
  public BinaryOperator<TranslationList> combiner() {
    return (left, right) -> {
      left.addAll(right);
      return left;
    };
  }

  @Override
  public Function<TranslationList, TranslationList> finisher() {
    return Function.identity();
  }

  @Override
  public Set<Characteristics> characteristics() {
    return Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH));
  }

}
