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

package au.csiro.pathling.export.fhir;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Parameters {

  @Value
  @Builder
  public static class Parameter {

    @Nonnull
    String name;

    @Nullable
    @Builder.Default
    Reference valueReference = null;

    @Nullable
    @Builder.Default
    String valueString = null;

    @Nullable
    @Builder.Default
    String valueInstant = null;

    @Nonnull
    public static Parameter of(@Nonnull final String name,
        final @Nonnull Reference valueReference) {
      return Parameter.builder().name(name).valueReference(valueReference).build();
    }

    @Nonnull
    public static Parameter of(@Nonnull final String name, final @Nonnull String valueString) {
      return Parameter.builder().name(name).valueString(valueString).build();
    }

    @Nonnull
    public static Parameter of(@Nonnull final String name, final @Nonnull Instant valueInstant) {
      return Parameter.builder().name(name).valueInstant(FhirUtils.formatFhirInstant(valueInstant))
          .build();
    }
  }

  @Nonnull
  @Builder.Default
  String resourceType = "Parameters";

  @Nonnull
  @Builder.Default
  List<Parameter> parameter = Collections.emptyList();
}
