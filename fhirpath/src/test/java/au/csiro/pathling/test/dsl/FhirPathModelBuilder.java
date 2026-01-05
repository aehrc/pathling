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

package au.csiro.pathling.test.dsl;

import au.csiro.pathling.test.yaml.FhirTypedLiteral;
import au.csiro.pathling.test.yaml.YamlSupport;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.jetbrains.annotations.Contract;

@RequiredArgsConstructor
public class FhirPathModelBuilder {

  @Getter private final Map<String, Object> model = new HashMap<>();

  public FhirPathModelBuilder fhirType(@Nonnull final FHIRDefinedType fhirType) {
    model.put(YamlSupport.FHIR_TYPE_ANNOTATION, fhirType.toCode());
    return this;
  }

  public FhirPathModelBuilder choice(@Nonnull final String name) {
    model.put(YamlSupport.CHOICE_ANNOTATION, name);
    return this;
  }

  public FhirPathModelBuilder string(@Nonnull final String name, @Nullable final String value) {
    model.put(name, value);
    return this;
  }

  public FhirPathModelBuilder stringEmpty(@Nonnull final String name) {
    model.put(name, FhirTypedLiteral.toString(null));
    return this;
  }

  public FhirPathModelBuilder stringArray(@Nonnull final String name, final String... values) {
    model.put(name, Arrays.asList(values));
    return this;
  }

  public FhirPathModelBuilder integer(@Nonnull final String name, @Nullable final Integer value) {
    model.put(name, value);
    return this;
  }

  public FhirPathModelBuilder integerEmpty(@Nonnull final String name) {
    model.put(name, FhirTypedLiteral.toInteger(null));
    return this;
  }

  public FhirPathModelBuilder integerArray(
      @Nonnull final String name, @Nonnull final int... values) {
    final List<Integer> list = new ArrayList<>();
    for (final int value : values) {
      list.add(value);
    }
    model.put(name, list);
    return this;
  }

  public FhirPathModelBuilder decimal(@Nonnull final String name, @Nullable final Double value) {
    model.put(name, value);
    return this;
  }

  public FhirPathModelBuilder decimalEmpty(@Nonnull final String name) {
    model.put(name, FhirTypedLiteral.toDecimal(null));
    return this;
  }

  public FhirPathModelBuilder decimalArray(
      @Nonnull final String name, @Nonnull final double... values) {
    final List<Double> list = new ArrayList<>();
    for (final double value : values) {
      list.add(value);
    }
    model.put(name, list);
    return this;
  }

  public FhirPathModelBuilder bool(@Nonnull final String name, @Nullable final Boolean value) {
    model.put(name, value);
    return this;
  }

  public FhirPathModelBuilder boolEmpty(@Nonnull final String name) {
    model.put(name, FhirTypedLiteral.toBoolean(null));
    return this;
  }

  public FhirPathModelBuilder boolArray(final String name, @Nonnull final boolean... values) {
    final List<Boolean> list = new ArrayList<>();
    for (final boolean value : values) {
      list.add(value);
    }
    model.put(name, list);
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder time(@Nonnull final String name, @Nullable final String value) {
    model.put(name, FhirTypedLiteral.toTime(value));
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder timeEmpty(@Nonnull final String name) {
    model.put(name, FhirTypedLiteral.toTime(null));
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder timeArray(
      @Nonnull final String name, @Nonnull final String... values) {
    model.put(name, Stream.of(values).map(FhirTypedLiteral::toTime).toList());
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder date(@Nonnull final String name, @Nullable final String value) {
    model.put(name, FhirTypedLiteral.toDate(value));
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder dateEmpty(@Nonnull final String name) {
    model.put(name, FhirTypedLiteral.toDate(null));
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder dateArray(
      @Nonnull final String name, @Nonnull final String... values) {
    model.put(name, Stream.of(values).map(FhirTypedLiteral::toDate).toList());
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder dateTime(@Nonnull final String name, @Nullable final String value) {
    model.put(name, FhirTypedLiteral.toDateTime(value));
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder dateTimeEmpty(@Nonnull final String name) {
    model.put(name, FhirTypedLiteral.toDateTime(null));
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder dateTimeArray(
      @Nonnull final String name, @Nonnull final String... values) {
    model.put(name, Stream.of(values).map(FhirTypedLiteral::toDateTime).toList());
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder coding(@Nonnull final String name, @Nullable final String value) {
    model.put(name, FhirTypedLiteral.toCoding(value));
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder codingEmpty(@Nonnull final String name) {
    model.put(name, FhirTypedLiteral.toCoding(null));
    return this;
  }

  @Nonnull
  public FhirPathModelBuilder codingArray(
      @Nonnull final String name, @Nonnull final String... values) {
    model.put(name, Stream.of(values).map(FhirTypedLiteral::toCoding).toList());
    return this;
  }

  public FhirPathModelBuilder element(
      final String name, @Nonnull final Consumer<FhirPathModelBuilder> builderConsumer) {
    final FhirPathModelBuilder builder = new FhirPathModelBuilder();
    builderConsumer.accept(builder);
    model.put(name, builder.model);
    return this;
  }

  public FhirPathModelBuilder elementEmpty(@Nonnull final String name) {
    model.put(name, null);
    return this;
  }

  @Contract("_, _ -> this")
  @SafeVarargs
  public final FhirPathModelBuilder elementArray(
      final String name, @Nonnull final Consumer<FhirPathModelBuilder>... builders) {
    final List<Map<String, Object>> list = new ArrayList<>();
    for (final Consumer<FhirPathModelBuilder> builderConsumer : builders) {
      final FhirPathModelBuilder builder = new FhirPathModelBuilder();
      builderConsumer.accept(builder);
      list.add(builder.model);
    }
    model.put(name, list);
    return this;
  }

  public Map<String, Object> build() {
    return Collections.unmodifiableMap(model);
  }
}
