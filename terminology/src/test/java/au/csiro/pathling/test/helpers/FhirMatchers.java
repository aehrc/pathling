/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.helpers;

import static au.csiro.pathling.fhirpath.CodingHelpers.codingEquals;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.Coding;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

public final class FhirMatchers {

  private FhirMatchers() {}

  static class FhirDeepMatcher<T extends Base> implements ArgumentMatcher<T>, Serializable {

    @Serial private static final long serialVersionUID = -7388475686640982425L;

    @Nonnull private final T expected;

    private FhirDeepMatcher(@Nonnull final T expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(@Nullable final T actual) {
      return expected.equalsDeep(actual);
    }
  }

  private record CodingMatcher(@Nonnull Coding expected)
      implements ArgumentMatcher<Coding>, Serializable {

    @Override
    public boolean matches(@Nullable final Coding actual) {
      return codingEquals(expected, actual);
    }
  }

  public static <T extends Base> T deepEq(@Nonnull final T expected) {
    return Mockito.argThat(new FhirDeepMatcher<>(expected));
  }

  public static Coding codingEq(@Nonnull final Coding expected) {
    return Mockito.argThat(new CodingMatcher(expected));
  }
}
