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

package au.csiro.pathling.terminology.caching;

import au.csiro.pathling.terminology.PropertyOrDesignationList;
import au.csiro.pathling.terminology.TerminologyService.PropertyOrDesignation;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * A collector that collects a stream of items into a {@link PropertyOrDesignationList}.
 *
 * <p>This is the same as {@link java.util.stream.Collectors#toList()} except that it returns an
 * {@link PropertyOrDesignationList} instead of a generic {@link java.util.List}.
 *
 * <p>We need this because the list implementation needs to be constrained to ensure that it is
 * serializable for persistent caching purposes.
 */
public class PropertyOrDesignationListCollector
    implements Collector<
        PropertyOrDesignation, PropertyOrDesignationList, PropertyOrDesignationList> {

  @Override
  public Supplier<PropertyOrDesignationList> supplier() {
    return PropertyOrDesignationList::new;
  }

  @Override
  public BiConsumer<PropertyOrDesignationList, PropertyOrDesignation> accumulator() {
    return PropertyOrDesignationList::add;
  }

  @Override
  public BinaryOperator<PropertyOrDesignationList> combiner() {
    return (left, right) -> {
      left.addAll(right);
      return left;
    };
  }

  @Override
  public Function<PropertyOrDesignationList, PropertyOrDesignationList> finisher() {
    return Function.identity();
  }

  @Override
  public Set<Characteristics> characteristics() {
    return Collections.unmodifiableSet(EnumSet.of(Characteristics.IDENTITY_FINISH));
  }
}
